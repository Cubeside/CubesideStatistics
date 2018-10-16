package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.UUID;
import java.util.logging.Level;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.AchivementKey;
import de.iani.cubesidestats.api.Callback;
import de.iani.cubesidestats.api.PlayerStatistics;
import de.iani.cubesidestats.api.StatisticKey;
import de.iani.cubesidestats.api.TimeFrame;

public class PlayerStatisticsImplementation implements PlayerStatistics {
    private CubesideStatisticsImplementation stats;
    private final UUID playerId;
    private int databaseId;

    public PlayerStatisticsImplementation(CubesideStatisticsImplementation stats, UUID player) {
        if (player == null) {
            throw new NullPointerException("player");
        }
        if (stats == null) {
            throw new NullPointerException("stats");
        }
        this.stats = stats;
        playerId = player;
        databaseId = -1;
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    databaseId = database.getOrCreatePlayerId(player);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not load database id for " + playerId, e);
                }
            }
        });
    }

    @Override
    public UUID getOwner() {
        return playerId;
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
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    database.increaseScore(databaseId, (StatisticKeyImplementation) key, month, daykey, amount);
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
                    database.setScore(databaseId, (StatisticKeyImplementation) key, month, daykey, value);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set score for " + playerId, e);
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
                    boolean result = database.maxScore(databaseId, (StatisticKeyImplementation) key, month, daykey, value);
                    if (updatedCallback != null) {
                        stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                            @Override
                            public void run() {
                                updatedCallback.call(result);
                            }
                        });
                    }
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
                    boolean result = database.minScore(databaseId, (StatisticKeyImplementation) key, month, daykey, value);
                    if (updatedCallback != null) {
                        stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                            @Override
                            public void run() {
                                updatedCallback.call(result);
                            }
                        });
                    }
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
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    Integer score = database.getScore(databaseId, (StatisticKeyImplementation) key, month);
                    stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            scoreCallback.call(score);
                        }
                    });
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get score for " + playerId, e);
                }
            }
        });
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
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    Integer score = database.getScore(databaseId, (StatisticKeyImplementation) key, month);
                    stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            scoreCallback.call(score);
                        }
                    });
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get score for " + playerId, e);
                }
            }
        });
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
                    if (updatedCallback != null && (oldLevel == null || level != oldLevel)) {
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
                    if (updatedCallback != null && oldLevel != null && oldLevel > 0) {
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
                    stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            achivementCallback.call(level > 0);
                        }
                    });
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
                if (databaseId < 0) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Invalid database id for " + playerId);
                    return;
                }
                try {
                    Integer level = database.getAchivementLevel(databaseId, (AchivementKeyImplementation) key);
                    stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            achivementCallback.call(level);
                        }
                    });
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get achivement " + key.getName() + " for " + playerId, e);
                }
            }
        });
    }

}
