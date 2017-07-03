package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.logging.Level;

import org.bukkit.entity.Player;

import de.iani.cubesidestats.StatisticsDatabase.ConfigDTO;
import de.iani.cubesidestats.api.CubesideStatisticsAPI;
import de.iani.cubesidestats.api.PlayerStatistics;
import de.iani.cubesidestats.api.StatisticKey;

public class CubesideStatisticsImplementation implements CubesideStatisticsAPI {
    private HashMap<String, StatisticKeyImplementation> statisticKeys;
    private HashMap<UUID, PlayerStatisticsImplementation> onlinePlayers;
    private HashMap<UUID, TimestampedValue<PlayerStatisticsImplementation>> offlinePlayers;
    private StatisticsDatabase database;
    private CubesideStatistics plugin;
    private WorkerThread workerThread;
    private int configSerial = -1;
    private final static int CONFIG_RELOAD_TICKS = 20 * 60 * 5;// 5 minutes (in ticks)
    private final static long MIN_CACHE_NANOS = 1_000_000_000L * 60 * 5;// 5 minutes (in nanos)

    public CubesideStatisticsImplementation(CubesideStatistics plugin) throws SQLException {
        this.plugin = plugin;
        database = new StatisticsDatabase(plugin, new SQLConfig(plugin.getConfig().getConfigurationSection("database")));

        statisticKeys = new HashMap<>();
        onlinePlayers = new HashMap<>();
        offlinePlayers = new HashMap<>();

        reloadConfigNow();
        plugin.getServer().getScheduler().runTaskTimer(plugin, new Runnable() {
            @Override
            public void run() {
                cleanupCache();
                reloadConfig();
            }
        }, CONFIG_RELOAD_TICKS, CONFIG_RELOAD_TICKS);

        plugin.getServer().getPluginManager().registerEvents(new PlayerListener(this), plugin);

        workerThread = new WorkerThread();
        workerThread.start();
    }

    public void shutdown() {
        if (workerThread != null) {
            workerThread.shutdown();
        }
    }

    public WorkerThread getWorkerThread() {
        return workerThread;
    }

    public CubesideStatistics getPlugin() {
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
                    Collection<StatisticKeyImplementation> keys = config.getStatisticKeys();
                    for (StatisticKeyImplementation e : keys) {
                        StatisticKeyImplementation old = statisticKeys.get(e.getName());
                        if (old != null) {
                            old.copyPropertiesFrom(e);
                        } else {
                            statisticKeys.put(e.getName(), e);
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
    public PlayerStatistics getStatistics(UUID owner) {
        PlayerStatisticsImplementation stats = onlinePlayers.get(owner);
        if (stats != null) {
            return stats;
        }
        TimestampedValue<PlayerStatisticsImplementation> timestampedStats = offlinePlayers.get(owner);
        if (timestampedStats != null) {
            return timestampedStats.get();
        }
        stats = new PlayerStatisticsImplementation(this, owner);
        offlinePlayers.put(owner, new TimestampedValue<PlayerStatisticsImplementation>(stats));
        return stats;
    }

    private void cleanupCache() {
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

    @Override
    public StatisticKey getStatisticKey(String id) {
        return statisticKeys.get(id);
    }

    @Override
    public boolean hasStatisticKey(String id) {
        return statisticKeys.containsKey(id);
    }

    public void playerJoined(Player player) {
        TimestampedValue<PlayerStatisticsImplementation> old = offlinePlayers.remove(player.getUniqueId());
        onlinePlayers.put(player.getUniqueId(), old == null ? new PlayerStatisticsImplementation(this, player.getUniqueId()) : old.get());
    }

    public void playerDisconnected(Player player) {
        PlayerStatisticsImplementation old = onlinePlayers.remove(player.getUniqueId());
        if (old != null) {
            offlinePlayers.put(player.getUniqueId(), new TimestampedValue<PlayerStatisticsImplementation>(old));
        }
    }

    public class WorkerThread extends Thread {

        private StatisticsDatabase database;

        private boolean stopping;

        private ArrayDeque<WorkEntry> work;

        public WorkerThread() {
            work = new ArrayDeque<WorkEntry>();
            this.database = CubesideStatisticsImplementation.this.database;
        }

        public void addWork(WorkEntry e) {
            synchronized (work) {
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
                    } catch (Exception er) {
                        plugin.getLogger().log(Level.SEVERE, "Error in Statistics", er);
                    }
                }
            }
        }
    }

    public static interface WorkEntry {
        void process(StatisticsDatabase database);
    }
}
