package de.iani.cubesidestats;

import com.google.common.base.Preconditions;
import de.iani.cubesidestats.StatisticsDatabase.ConfigDTO;
import de.iani.cubesidestats.api.AchivementKey;
import de.iani.cubesidestats.api.Callback;
import de.iani.cubesidestats.api.CubesideStatisticsAPI;
import de.iani.cubesidestats.api.GamePlayerCount;
import de.iani.cubesidestats.api.GlobalStatisticKey;
import de.iani.cubesidestats.api.GlobalStatistics;
import de.iani.cubesidestats.api.GlobalStatisticsQueryKey;
import de.iani.cubesidestats.api.PlayerAchivementQueryKey;
import de.iani.cubesidestats.api.PlayerStatistics;
import de.iani.cubesidestats.api.PlayerStatisticsQueryKey;
import de.iani.cubesidestats.api.PlayerStatisticsQueryKey.QueryType;
import de.iani.cubesidestats.api.SettingKey;
import de.iani.cubesidestats.api.StatisticKey;
import de.iani.cubesidestats.api.StatisticsQueryKey;
import de.iani.cubesidestats.api.TimeFrame;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.logging.Level;
import org.bukkit.entity.Player;
import org.bukkit.event.HandlerList;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitTask;

public class CubesideStatisticsImplementation implements CubesideStatisticsAPI {
    private GlobalStatisticsImplementation globalStatistics;
    private ConcurrentHashMap<String, GlobalStatisticKeyImplementation> globalStatisticKeys;
    private ConcurrentHashMap<String, StatisticKeyImplementation> statisticKeys;
    private ConcurrentHashMap<String, AchivementKeyImplementation> achivementKeys;
    private ConcurrentHashMap<String, SettingKeyImplementation> settingKeys;
    private HashMap<UUID, PlayerStatisticsImplementation> onlinePlayers;
    private HashMap<UUID, TimestampedValue<PlayerStatisticsImplementation>> offlinePlayers;
    private StatisticsDatabase database;
    private Plugin plugin;
    private WorkerThread workerThread;
    private int configSerial = -1;
    private final static int CONFIG_RELOAD_TICKS = 20 * 60 * 5;// 5 minutes (in ticks)
    private final static long MIN_CACHE_NANOS = 1_000_000_000L * 60 * 5;// 5 minutes (in nanos)
    private final Calendar calender = Calendar.getInstance();
    private final UUID serverid;
    private final GamePlayerCountImplementation gamePlayerCount;
    private final Object playerListSync = new Object();
    private final Object keySync = new Object();
    private final BukkitTask reloadConfigTimer;
    private final PlayerListener listener;
    private final boolean internal;

    public CubesideStatisticsImplementation(Plugin plugin, SQLConfig config) throws SQLException {
        internal = plugin instanceof CubesideStatistics;
        this.plugin = plugin;
        serverid = loadOrCreateServerId();
        database = new StatisticsDatabase(this, config);

        globalStatisticKeys = new ConcurrentHashMap<>();
        statisticKeys = new ConcurrentHashMap<>();
        achivementKeys = new ConcurrentHashMap<>();
        settingKeys = new ConcurrentHashMap<>();
        onlinePlayers = new HashMap<>();
        offlinePlayers = new HashMap<>();

        reloadConfigNow();
        reloadConfigTimer = plugin.getServer().getScheduler().runTaskTimerAsynchronously(plugin, new Runnable() {
            @Override
            public void run() {
                cleanupCache();
                reloadConfig();
            }
        }, CONFIG_RELOAD_TICKS, CONFIG_RELOAD_TICKS);
        if (internal) {
            plugin.getServer().getPluginManager().registerEvents(listener = new PlayerListener(this), plugin);
        } else {
            listener = null;
        }

        workerThread = new WorkerThread();
        workerThread.start();

        gamePlayerCount = new GamePlayerCountImplementation(this);
        globalStatistics = new GlobalStatisticsImplementation(this);
    }

    private UUID loadOrCreateServerId() {
        File serveridFile = new File(plugin.getDataFolder().getParentFile().getParentFile(), "serverid");
        String serveridstring = null;
        if (serveridFile.isFile()) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(serveridFile));
                serveridstring = reader.readLine();
                reader.close();
            } catch (IOException e) {
                // ignore
            }
        }
        UUID localServerid = null;
        try {
            localServerid = serveridstring == null ? null : UUID.fromString(serveridstring);
        } catch (IllegalArgumentException e) {
            // ignored
        }
        if (localServerid == null) {
            plugin.getLogger().info("Keine gültige Server-ID vorhanden! Generiere neue ID!");
            localServerid = UUID.randomUUID();
            try {
                FileWriter writer = new FileWriter(serveridFile);
                writer.write(localServerid.toString());
                writer.flush();
                writer.close();
            } catch (IOException e) {
                plugin.getLogger().log(Level.SEVERE, "Could not save server id file", e);
            }
        }
        return localServerid;
    }

    public void shutdown() {
        if (listener != null) {
            HandlerList.unregisterAll(listener);
        }
        if (reloadConfigTimer != null) {
            reloadConfigTimer.cancel();
        }
        if (workerThread != null) {
            gamePlayerCount.clearLocalPlayers();
            workerThread.shutdown();
        }
    }

    public WorkerThread getWorkerThread() {
        return workerThread;
    }

    public Plugin getPlugin() {
        return plugin;
    }

    private void reloadConfig() {
        getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                reloadConfigNow();
            }
        });
    }

    protected void reloadConfigNow() {
        try {
            ConfigDTO config = database.loadConfig(configSerial);
            if (config == null) {
                return; // there is no newer config to load
            }
            Runnable mainTheadLogic = new Runnable() {
                @Override
                public void run() {
                    if (config.getConfigSerial() <= configSerial) {
                        return; // we already have the current config
                    }
                    configSerial = config.getConfigSerial();
                    synchronized (keySync) {
                        Collection<GlobalStatisticKeyImplementation> newGlobalStatisticKeys = config.getGlobalStatisticKeys();
                        for (GlobalStatisticKeyImplementation e : newGlobalStatisticKeys) {
                            GlobalStatisticKeyImplementation old = globalStatisticKeys.get(e.getName());
                            if (old != null) {
                                old.copyPropertiesFrom(e);
                            } else {
                                globalStatisticKeys.put(e.getName(), e);
                            }
                        }
                        Collection<StatisticKeyImplementation> newStatisticKeys = config.getStatisticKeys();
                        for (StatisticKeyImplementation e : newStatisticKeys) {
                            StatisticKeyImplementation old = statisticKeys.get(e.getName());
                            if (old != null) {
                                old.copyPropertiesFrom(e);
                            } else {
                                statisticKeys.put(e.getName(), e);
                            }
                        }
                        Collection<AchivementKeyImplementation> newAchivementKeys = config.getAchivementKeys();
                        for (AchivementKeyImplementation e : newAchivementKeys) {
                            AchivementKeyImplementation old = achivementKeys.get(e.getName());
                            if (old != null) {
                                old.copyPropertiesFrom(e);
                            } else {
                                achivementKeys.put(e.getName(), e);
                            }
                        }
                        Collection<SettingKeyImplementation> newSettingKeys = config.getSettingKeys();
                        for (SettingKeyImplementation e : newSettingKeys) {
                            SettingKeyImplementation old = settingKeys.get(e.getName());
                            if (old != null) {
                                old.copyPropertiesFrom(e);
                            } else {
                                settingKeys.put(e.getName(), e);
                            }
                        }
                    }
                    plugin.getLogger().info("Reloaded config from the database");
                }
            };
            if (plugin.getServer().isPrimaryThread()) {
                mainTheadLogic.run();
            } else {
                getPlugin().getServer().getScheduler().runTask(getPlugin(), mainTheadLogic);
            }
        } catch (SQLException e) {
            getPlugin().getLogger().log(Level.SEVERE, "Could not reload the config from the database", e);
        }
    }

    @Override
    public GlobalStatistics getGlobalStatistics() {
        return globalStatistics;
    }

    @Override
    public PlayerStatistics getStatistics(UUID owner) {
        synchronized (playerListSync) {
            PlayerStatisticsImplementation stats = onlinePlayers.get(owner);
            if (stats != null) {
                return stats;
            }
            TimestampedValue<PlayerStatisticsImplementation> timestampedStats = offlinePlayers.get(owner);
            if (timestampedStats != null) {
                return timestampedStats.get();
            }
            stats = new PlayerStatisticsImplementation(this, owner, null);
            offlinePlayers.put(owner, new TimestampedValue<>(stats));
            return stats;
        }
    }

    private void cleanupCache() {
        synchronized (playerListSync) {
            if (!offlinePlayers.isEmpty()) {
                long minTimestamp = System.nanoTime() - MIN_CACHE_NANOS;
                Iterator<TimestampedValue<PlayerStatisticsImplementation>> it = offlinePlayers.values().iterator();
                while (it.hasNext()) {
                    TimestampedValue<PlayerStatisticsImplementation> current = it.next();
                    if (current.getTimestamp() < minTimestamp) {
                        it.remove();
                    }
                }
            }
        }
    }

    @Override
    public GlobalStatisticKey getGlobalStatisticKey(String id) {
        return getGlobalStatisticKey(id, true);
    }

    @Override
    public GlobalStatisticKey getGlobalStatisticKey(String id, boolean create) {
        GlobalStatisticKeyImplementation existing = globalStatisticKeys.get(id);
        if (existing == null && create) {
            reloadConfigNow();
            synchronized (keySync) {
                existing = globalStatisticKeys.get(id);
                if (existing == null) {
                    try {
                        existing = database.createGlobalStatisticKey(id);
                        globalStatisticKeys.put(existing.getName(), existing);
                    } catch (SQLException e) {
                        plugin.getLogger().log(Level.SEVERE, "Could not create global statistics key", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return existing;
    }

    @Override
    public boolean hasGlobalStatisticKey(String id) {
        return globalStatisticKeys.containsKey(id);
    }

    @Override
    public Collection<? extends GlobalStatisticKey> getAllGlobalStatisticKeys() {
        return Collections.unmodifiableCollection(globalStatisticKeys.values());
    }

    @Override
    public StatisticKey getStatisticKey(String id) {
        return getStatisticKey(id, true);
    }

    @Override
    public StatisticKey getStatisticKey(String id, boolean create) {
        StatisticKeyImplementation existing = statisticKeys.get(id);
        if (existing == null && create) {
            reloadConfigNow();
            synchronized (keySync) {
                existing = statisticKeys.get(id);
                if (existing == null) {
                    try {
                        existing = database.createStatisticKey(id);
                        statisticKeys.put(existing.getName(), existing);
                    } catch (SQLException e) {
                        plugin.getLogger().log(Level.SEVERE, "Could not create statistics key", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return existing;
    }

    @Override
    public boolean hasStatisticKey(String id) {
        return statisticKeys.containsKey(id);
    }

    @Override
    public Collection<? extends StatisticKey> getAllStatisticKeys() {
        return Collections.unmodifiableCollection(statisticKeys.values());
    }

    public void playerJoined(Player player) {
        synchronized (playerListSync) {
            TimestampedValue<PlayerStatisticsImplementation> old = offlinePlayers.remove(player.getUniqueId());
            onlinePlayers.put(player.getUniqueId(), old == null ? new PlayerStatisticsImplementation(this, player.getUniqueId(), settingKeys.values()) : old.get().reloadSettingsAsync(settingKeys.values()));
        }
    }

    public void playerDisconnected(Player player) {
        synchronized (playerListSync) {
            PlayerStatisticsImplementation old = onlinePlayers.remove(player.getUniqueId());
            if (old != null) {
                offlinePlayers.put(player.getUniqueId(), new TimestampedValue<>(old));
            }
        }
    }

    public class WorkerThread extends Thread {

        private StatisticsDatabase database;

        private boolean stopping;

        private ArrayDeque<WorkEntry> work;

        public WorkerThread() {
            work = new ArrayDeque<>();
            this.database = CubesideStatisticsImplementation.this.database;
        }

        public void addWork(WorkEntry e) {
            synchronized (work) {
                if (stopping) {
                    return;
                }
                work.addLast(e);
                work.notify();
            }
        }

        public void shutdown() {
            synchronized (work) {
                stopping = true;
                work.notify();
            }
            boolean interrupt = false;
            while (isAlive()) {
                try {
                    join();
                } catch (InterruptedException e) {
                    interrupt = true;
                }
            }
            if (interrupt) {
                Thread.currentThread().interrupt();
            }
            if (database != null) {
                database.disconnect();
                database = null;
            }
        }

        @Override
        public void run() {
            WorkEntry e;
            while (true) {
                synchronized (work) {
                    e = work.pollFirst();
                    if (e == null) {
                        if (stopping) {
                            return;
                        }
                        try {
                            work.wait();
                        } catch (InterruptedException e1) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
                if (e != null && database != null) {
                    try {
                        e.process(database);
                    } catch (Throwable er) {
                        plugin.getLogger().log(Level.SEVERE, "Error in Statistics", er);
                    }
                }
            }
        }
    }

    public static interface WorkEntry {
        void process(StatisticsDatabase database);
    }

    public int getCurrentMonthKey() {
        calender.setTimeInMillis(System.currentTimeMillis());
        return getMonthKey(calender);
    }

    public int getCurrentDayKey() {
        calender.setTimeInMillis(System.currentTimeMillis());
        return getDayKey(calender);
    }

    public static int getMonthKey(Calendar calender) {
        return calender.get(Calendar.YEAR) * 100 + calender.get(Calendar.MONTH) + 1;
    }

    public static int getDayKey(Calendar calender) {
        return calender.get(Calendar.YEAR) * 1000 + calender.get(Calendar.DAY_OF_YEAR);
    }

    public UUID getServerId() {
        return serverid;
    }

    @Override
    public GamePlayerCount getGamePlayerCount() {
        return gamePlayerCount;
    }

    @Override
    public AchivementKey getAchivementKey(String id) {
        return getAchivementKey(id, true);
    }

    @Override
    public AchivementKey getAchivementKey(String id, boolean create) {
        AchivementKeyImplementation existing = achivementKeys.get(id);
        if (existing == null && create) {
            reloadConfigNow();
            synchronized (keySync) {
                existing = achivementKeys.get(id);
                if (existing == null) {
                    try {
                        existing = database.createAchivementKey(id);
                        achivementKeys.put(existing.getName(), existing);
                    } catch (SQLException e) {
                        plugin.getLogger().log(Level.SEVERE, "Could not create statistics key", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return existing;
    }

    @Override
    public Collection<? extends AchivementKey> getAllAchivementKeys() {
        return Collections.unmodifiableCollection(achivementKeys.values());
    }

    @Override
    public boolean hasAchivementKey(String id) {
        return achivementKeys.containsKey(id);
    }

    @Override
    public SettingKey getSettingKey(String id) {
        return getSettingKey(id, true);
    }

    @Override
    public SettingKey getSettingKey(String id, boolean create) {
        SettingKeyImplementation existing = settingKeys.get(id);
        if (existing == null && create) {
            reloadConfigNow();
            synchronized (keySync) {
                existing = settingKeys.get(id);
                if (existing == null) {
                    try {
                        existing = database.createSettingKey(id);
                        settingKeys.put(existing.getName(), existing);
                    } catch (SQLException e) {
                        plugin.getLogger().log(Level.SEVERE, "Could not create setting key", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return existing;
    }

    @Override
    public boolean hasSettingKey(String id) {
        return settingKeys.containsKey(id);
    }

    @Override
    public Collection<? extends SettingKey> getAllSettingKeys() {
        return Collections.unmodifiableCollection(settingKeys.values());
    }

    @Override
    public Future<Map<StatisticsQueryKey, Integer>> queryStats(Collection<StatisticsQueryKey> querys) {
        return queryStatsInternal(querys, null);
    }

    @Override
    public void queryStats(Collection<StatisticsQueryKey> querys, Callback<Map<StatisticsQueryKey, Integer>> callback) {
        queryStatsInternal(querys, callback);
    }

    private Future<Map<StatisticsQueryKey, Integer>> queryStatsInternal(Collection<StatisticsQueryKey> querys, Callback<Map<StatisticsQueryKey, Integer>> callback) {
        Preconditions.checkNotNull(querys, "querys");
        ArrayList<StatisticsQueryKey> internalQuerys = new ArrayList<>(querys);
        int currentMonthKey = getCurrentMonthKey();
        int currentDayKey = getCurrentDayKey();

        CompletableFuture<Map<StatisticsQueryKey, Integer>> future = new CompletableFuture<>();

        getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                HashMap<StatisticsQueryKey, Integer> result = new HashMap<>();
                for (StatisticsQueryKey queryKey : internalQuerys) {
                    if (queryKey instanceof PlayerStatisticsQueryKey) {
                        PlayerStatisticsQueryKey playerQueryKey = (PlayerStatisticsQueryKey) queryKey;
                        if (playerQueryKey.getPlayer() instanceof PlayerStatisticsImplementation) {
                            int timeKey = -1;
                            if (playerQueryKey.getTimeFrame() == TimeFrame.MONTH) {
                                timeKey = currentMonthKey;
                            } else if (playerQueryKey.getTimeFrame() == TimeFrame.DAY) {
                                timeKey = currentDayKey;
                            }
                            PlayerStatisticsImplementation player = (PlayerStatisticsImplementation) playerQueryKey.getPlayer();
                            if (playerQueryKey.getType() == QueryType.POSITION_MAX) {
                                Integer pos = player.internalGetPositionMaxInMonth(database, playerQueryKey.getKey(), timeKey);
                                if (pos != null) {
                                    result.put(queryKey, pos);
                                }
                            } else if (playerQueryKey.getType() == QueryType.POSITION_MIN) {
                                Integer pos = player.internalGetPositionMinInMonth(database, playerQueryKey.getKey(), timeKey);
                                if (pos != null) {
                                    result.put(queryKey, pos);
                                }
                            } else if (playerQueryKey.getType() == QueryType.POSITION_MAX_TOTAL_ORDER) {
                                Integer pos = player.internalGetPositionMaxTotalOrderInMonth(database, playerQueryKey.getKey(), timeKey);
                                if (pos != null) {
                                    result.put(queryKey, pos);
                                }
                            } else if (playerQueryKey.getType() == QueryType.POSITION_MIN_TOTAL_ORDER) {
                                Integer pos = player.internalGetPositionMinTotalOrderInMonth(database, playerQueryKey.getKey(), timeKey);
                                if (pos != null) {
                                    result.put(queryKey, pos);
                                }
                            } else if (playerQueryKey.getType() == QueryType.SCORE) {
                                Integer score = player.internalGetScoreInMonth(database, playerQueryKey.getKey(), timeKey);
                                if (score != null) {
                                    result.put(queryKey, score);
                                }
                            }
                        }
                    } else if (queryKey instanceof PlayerAchivementQueryKey) {
                        PlayerAchivementQueryKey playerQueryKey = (PlayerAchivementQueryKey) queryKey;
                        if (playerQueryKey.getPlayer() instanceof PlayerStatisticsImplementation) {
                            PlayerStatisticsImplementation player = (PlayerStatisticsImplementation) playerQueryKey.getPlayer();
                            Integer level = player.internalGetAchivementLevel(database, playerQueryKey.getKey());
                            if (level != null) {
                                result.put(queryKey, level);
                            }
                        }
                    } else if (queryKey instanceof GlobalStatisticsQueryKey) {
                        GlobalStatisticsQueryKey globalQueryKey = (GlobalStatisticsQueryKey) queryKey;
                        int timeKey = -1;
                        if (globalQueryKey.getTimeFrame() == TimeFrame.MONTH) {
                            timeKey = currentMonthKey;
                        } else if (globalQueryKey.getTimeFrame() == TimeFrame.DAY) {
                            timeKey = currentDayKey;
                        }
                        Integer score = globalStatistics.internalGetScoreInMonth(database, globalQueryKey.getKey(), timeKey);
                        if (score != null) {
                            result.put(queryKey, score);
                        }
                    }
                }
                future.complete(result);
                if (callback != null && getPlugin().isEnabled()) {
                    getPlugin().getServer().getScheduler().runTask(getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            callback.call(result);
                        }
                    });
                }
            }
        });
        return future;
    }

    public boolean isInternal() {
        return internal;
    }
}
