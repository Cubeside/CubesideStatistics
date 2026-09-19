package de.iani.cubesidestats;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.StatisticsDatabase.StatsUpdateResultDTO;
import de.iani.cubesidestats.api.AchivementKey;
import de.iani.cubesidestats.api.Callback;
import de.iani.cubesidestats.api.PlayerStatistics;
import de.iani.cubesidestats.api.SettingKey;
import de.iani.cubesidestats.api.StatisticKey;
import de.iani.cubesidestats.api.TimeFrame;
import de.iani.cubesidestats.api.event.PlayerStatisticUpdatedEvent;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import org.bukkit.Bukkit;

public class PlayerStatisticsImplementation implements PlayerStatistics {
    private final CubesideStatisticsImplementation stats;
    private final UUID playerId;
    private volatile int databaseId;
    private final CompletableFuture<Void> databaseIdLoaded;
    private final Object settingsSync = new Object();
    private HashSet<SettingKeyImplementation> settingsChangedDuringLoad;
    private final HashMap<SettingKeyImplementation, Integer> settings;
    private volatile boolean settingsLoaded;
    private CompletableFuture<Void> settingsLoadFuture;

    public PlayerStatisticsImplementation(CubesideStatisticsImplementation stats, UUID player, Collection<SettingKeyImplementation> settingKeys) {
        if (player == null) {
            throw new NullPointerException("player");
        }
        if (stats == null) {
            throw new NullPointerException("stats");
        }
        this.stats = stats;
        playerId = player;
        databaseId = -1;
        this.settingsLoaded = false;
        this.settings = new HashMap<>();
        databaseIdLoaded = stats.getWorkerThread().submitWork(database -> {
            databaseId = database.getOrCreatePlayerId(player);
            return null;
        });
        databaseIdLoaded.whenComplete((ignored, error) -> {
            if (error != null) {
                stats.getPlugin().getLogger().log(Level.SEVERE, "Could not load database id for " + playerId, unwrapCompletionException(error));
            }
        });
        if (settingKeys != null) {
            reloadSettingsAsync(settingKeys);
        }
    }

    public PlayerStatisticsImplementation reloadSettingsAsync(Collection<SettingKeyImplementation> settingKeys) {
        reloadSettingsFuture(settingKeys);
        return this;
    }

    CompletableFuture<Void> reloadSettingsFuture(Collection<SettingKeyImplementation> settingKeys) {
        Collection<SettingKeyImplementation> settingKeysSnapshot = new ArrayList<>(settingKeys);
        synchronized (settingsSync) {
            if (settingsLoadFuture != null && !settingsLoadFuture.isDone()) {
                return settingsLoadFuture;
            }

            HashSet<SettingKeyImplementation> changedSettings = new HashSet<>();
            settingsChangedDuringLoad = changedSettings;
            CompletableFuture<Void> future = stats.getWorkerThread().submitWork(database -> {
                if (!stats.getPlugin().isEnabled()) {
                    throw new IllegalStateException("Statistics plugin is disabled");
                }
                databaseIdLoaded.join();
                if (databaseId < 0) {
                    throw new SQLException("Invalid database id for " + playerId);
                }

                HashMap<SettingKeyImplementation, Integer> loadedSettings = database.getSettingValues(databaseId, settingKeysSnapshot);
                if (!stats.getPlugin().isEnabled()) {
                    throw new IllegalStateException("Statistics plugin was disabled while loading settings");
                }
                synchronized (settingsSync) {
                    for (SettingKeyImplementation changedSetting : changedSettings) {
                        loadedSettings.put(changedSetting, settings.get(changedSetting));
                    }
                    settings.clear();
                    settings.putAll(loadedSettings);
                    settingsLoaded = true;
                    if (settingsChangedDuringLoad == changedSettings) {
                        settingsChangedDuringLoad = null;
                    }
                }
                return null;
            });
            settingsLoadFuture = future;
            future.whenComplete((ignored, error) -> {
                synchronized (settingsSync) {
                    if (settingsLoadFuture == future) {
                        settingsLoadFuture = null;
                    }
                    if (settingsChangedDuringLoad == changedSettings) {
                        settingsChangedDuringLoad = null;
                    }
                }
            });
            return future;
        }
    }

    public boolean isSettingsLoadInProgress() {
        synchronized (settingsSync) {
            return settingsLoadFuture != null && !settingsLoadFuture.isDone();
        }
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        Throwable result = error;
        while (result.getCause() != null && result instanceof java.util.concurrent.CompletionException) {
            result = result.getCause();
        }
        return result;
    }

    @Override
    public UUID getOwner() {
        return playerId;
    }

    protected void callUpdatedEventInMainThread(StatisticKey key, StatsUpdateResultDTO result) {
        if (stats.getPlugin().isEnabled()) {
            Bukkit.getScheduler().runTask(stats.getPlugin(), () -> new PlayerStatisticUpdatedEvent(playerId, key, result.getOldAlltime(), result.getNewAlltime(), result.getOldMonth(), result.getNewMonth(), result.getOldDay(), result.getNewDay()).callEvent());
        }
    }

    @Override
    public void decreaseScore(StatisticKey key, int amount) {
        increaseScore(key, -amount);
    }

    @Override
    public void increaseScore(StatisticKey key, int amount) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        final int month = stats.getCurrentMonthKey();
        final int daykey = stats.getCurrentDayKey();
        internalIncreaseScore(key, amount, month, daykey);
    }

    public void internalIncreaseScore(StatisticKey key, int amount, final int monthKey, final int dayKey) {
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    StatsUpdateResultDTO result = database.increaseScore(databaseId, (StatisticKeyImplementation) key, monthKey, dayKey, amount);
                    callUpdatedEventInMainThread(key, result);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not increase score for " + playerId, e);
                }
            }
        });
    }

    @Override
    public void setScore(StatisticKey key, int value) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        final int month = stats.getCurrentMonthKey();
        final int daykey = stats.getCurrentDayKey();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    StatsUpdateResultDTO result = database.setScore(databaseId, (StatisticKeyImplementation) key, month, daykey, value);
                    callUpdatedEventInMainThread(key, result);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set score for " + playerId, e);
                }
            }
        });
    }

    @Override
    public void deleteScore(StatisticKey key) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    database.deleteScore(databaseId, (StatisticKeyImplementation) key);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not delete score for " + playerId, e);
                }
            }
        });
    }

    @Override
    public void maxScore(StatisticKey key, int value) {
        maxScore(key, value, null);
    }

    @Override
    public void maxScore(StatisticKey key, int value, Callback<Boolean> updatedCallback) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        final int month = stats.getCurrentMonthKey();
        final int daykey = stats.getCurrentDayKey();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    StatsUpdateResultDTO result = database.maxScore(databaseId, (StatisticKeyImplementation) key, month, daykey, value);
                    if (updatedCallback != null && stats.getPlugin().isEnabled()) {
                        stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                            @Override
                            public void run() {
                                boolean updated = result.getOldAlltime() == null || result.getOldAlltime() < result.getNewAlltime();
                                updatedCallback.call(updated);
                            }
                        });
                    }
                    callUpdatedEventInMainThread(key, result);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set score for " + playerId, e);
                }
            }
        });
    }

    @Override
    public void minScore(StatisticKey key, int value) {
        minScore(key, value, null);
    }

    @Override
    public void minScore(StatisticKey key, int value, Callback<Boolean> updatedCallback) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        final int month = stats.getCurrentMonthKey();
        final int daykey = stats.getCurrentDayKey();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    StatsUpdateResultDTO result = database.minScore(databaseId, (StatisticKeyImplementation) key, month, daykey, value);
                    if (updatedCallback != null && stats.getPlugin().isEnabled()) {
                        stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                            @Override
                            public void run() {
                                boolean updated = result.getOldAlltime() == null || result.getOldAlltime() > result.getNewAlltime();
                                updatedCallback.call(updated);
                            }
                        });
                    }
                    callUpdatedEventInMainThread(key, result);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set score for " + playerId, e);
                }
            }
        });
    }

    @Override
    public void getScore(StatisticKey key, TimeFrame timeFrame, Callback<Integer> scoreCallback) {
        getScoreInMonth(key, getMonthKey(timeFrame), scoreCallback);
    }

    @Override
    public void getPosition(StatisticKey key, TimeFrame timeFrame, Callback<Integer> positionCallback) {
        getPositionInMonth(key, getMonthKey(timeFrame), positionCallback);
    }

    private int getMonthKey(TimeFrame timeFrame) {
        int month = -1;
        if (timeFrame == TimeFrame.MONTH) {
            month = stats.getCurrentMonthKey();
        } else if (timeFrame == TimeFrame.DAY) {
            month = stats.getCurrentDayKey();
        }
        return month;
    }

    private void getScoreInMonth(StatisticKey key, int month, Callback<Integer> scoreCallback) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        if (scoreCallback == null) {
            throw new NullPointerException("scoreCallback");
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                Integer score = internalGetScoreInMonth(database, key, month);
                if (stats.getPlugin().isEnabled()) {
                    stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            scoreCallback.call(score);
                        }
                    });
                }
            }
        });
    }

    protected Integer internalGetScoreInMonth(StatisticsDatabase database, StatisticKey key, int month) {
        if (databaseId < 0) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
            return null;
        }
        try {
            return database.getScore(databaseId, (StatisticKeyImplementation) key, month);
        } catch (SQLException e) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get score for " + playerId, e);
        }
        return null;
    }

    private void getPositionInMonth(StatisticKey key, int month, Callback<Integer> scoreCallback) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        if (scoreCallback == null) {
            throw new NullPointerException("scoreCallback");
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                Integer score = internalGetPositionMaxInMonth(database, key, month);
                if (score != null && stats.getPlugin().isEnabled()) {
                    stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            scoreCallback.call(score);
                        }
                    });
                }
            }
        });
    }

    protected Integer internalGetPositionMaxInMonth(StatisticsDatabase database, StatisticKey key, int month) {
        if (databaseId < 0) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
            return null;
        }
        try {
            return database.getPositionMax(databaseId, (StatisticKeyImplementation) key, month);
        } catch (SQLException e) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get position for score for " + playerId, e);
        }
        return null;
    }

    protected Integer internalGetPositionMinInMonth(StatisticsDatabase database, StatisticKey key, int month) {
        if (databaseId < 0) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
            return null;
        }
        try {
            return database.getPositionMin(databaseId, (StatisticKeyImplementation) key, month);
        } catch (SQLException e) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get position for score for " + playerId, e);
        }
        return null;
    }

    protected Integer internalGetPositionMaxTotalOrderInMonth(StatisticsDatabase database, StatisticKey key, int month) {
        if (databaseId < 0) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
            return null;
        }
        try {
            return database.getPositionMaxTotalOrder(databaseId, (StatisticKeyImplementation) key, month);
        } catch (SQLException e) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get position for score for " + playerId, e);
        }
        return null;
    }

    protected Integer internalGetPositionMinTotalOrderInMonth(StatisticsDatabase database, StatisticKey key, int month) {
        if (databaseId < 0) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
            return null;
        }
        try {
            return database.getPositionMinTotalOrder(databaseId, (StatisticKeyImplementation) key, month);
        } catch (SQLException e) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get position for score for " + playerId, e);
        }
        return null;
    }

    @Override
    public void grantAchivement(AchivementKey key) {
        grantAchivement(key, 1, null);
    }

    @Override
    public void grantAchivement(AchivementKey key, Callback<Integer> updatedCallback) {
        grantAchivement(key, 1, updatedCallback);
    }

    @Override
    public void grantAchivement(AchivementKey key, int level) {
        grantAchivement(key, level, null);
    }

    @Override
    public void grantAchivement(AchivementKey key, int level, Callback<Integer> updatedCallback) {
        if (!(key instanceof AchivementKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        if (level < 1 || level > key.getMaxLevel()) {
            throw new IllegalArgumentException("level");
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    Integer oldLevel = database.maxAchivementLevel(databaseId, (AchivementKeyImplementation) key, level, updatedCallback != null);
                    if (updatedCallback != null && (oldLevel == null || level != oldLevel) && stats.getPlugin().isEnabled()) {
                        stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                            @Override
                            public void run() {
                                updatedCallback.call(oldLevel);
                            }
                        });
                    }
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not grant achivement " + key.getName() + " for " + playerId, e);
                }
            }
        });
    }

    @Override
    public void revokeAchivement(AchivementKey key) {
        revokeAchivement(key, null);
    }

    @Override
    public void revokeAchivement(AchivementKey key, Callback<Integer> updatedCallback) {
        if (!(key instanceof AchivementKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    Integer oldLevel = database.setAchivementLevel(databaseId, (AchivementKeyImplementation) key, 0, updatedCallback != null);
                    if (updatedCallback != null && oldLevel != null && oldLevel > 0 && stats.getPlugin().isEnabled()) {
                        stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                            @Override
                            public void run() {
                                updatedCallback.call(oldLevel);
                            }
                        });
                    }
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not revoke achivement " + key.getName() + " for " + playerId, e);
                }
            }
        });
    }

    @Override
    public void hasAchivement(AchivementKey key, Callback<Boolean> achivementCallback) {
        if (!(key instanceof AchivementKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        if (achivementCallback == null) {
            throw new NullPointerException("achivementCallback");
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    Integer level = database.getAchivementLevel(databaseId, (AchivementKeyImplementation) key);
                    if (stats.getPlugin().isEnabled()) {
                        stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                            @Override
                            public void run() {
                                achivementCallback.call(level > 0);
                            }
                        });
                    }
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get achivement " + key.getName() + " for " + playerId, e);
                }
            }
        });
    }

    @Override
    public void getAchivementLevel(AchivementKey key, Callback<Integer> achivementCallback) {
        if (!(key instanceof AchivementKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        if (achivementCallback == null) {
            throw new NullPointerException("achivementCallback");
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                Integer level = internalGetAchivementLevel(database, key);
                if (level != null && stats.getPlugin().isEnabled()) {
                    stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            achivementCallback.call(level);
                        }
                    });
                }
            }
        });
    }

    protected Integer internalGetAchivementLevel(StatisticsDatabase database, AchivementKey key) {
        if (databaseId < 0) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
            return null;
        }
        try {
            return database.getAchivementLevel(databaseId, (AchivementKeyImplementation) key);
        } catch (SQLException e) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get achivement " + key.getName() + " for " + playerId, e);
        }
        return null;
    }

    @Override
    public boolean areSettingsLoaded() {
        return settingsLoaded;
    }

    @Override
    public Integer getSettingValueIfLoaded(SettingKey setting) {
        synchronized (settingsSync) {
            return settings.get(setting);
        }
    }

    @Override
    public int getSettingValueOrDefault(SettingKey setting) {
        Integer value = getSettingValueIfLoaded(setting);
        return value != null ? value.intValue() : setting.getDefault();
    }

    @Override
    public void setSettingValue(SettingKey key, int value) {
        if (!(key instanceof SettingKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        synchronized (settingsSync) {
            settings.put((SettingKeyImplementation) key, value);
            if (settingsChangedDuringLoad != null) {
                settingsChangedDuringLoad.add((SettingKeyImplementation) key);
            }
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    database.setSettingValue(databaseId, (SettingKeyImplementation) key, value, false);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set setting value for " + playerId, e);
                }
            }
        });
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != PlayerStatisticsImplementation.class) {
            return false;
        }
        PlayerStatisticsImplementation other = (PlayerStatisticsImplementation) obj;
        return playerId.equals(other.playerId);
    }

    @Override
    public int hashCode() {
        return playerId.hashCode();
    }
}
