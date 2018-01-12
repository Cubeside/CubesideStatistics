package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.UUID;
import java.util.logging.Level;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.AchivementKey;
import de.iani.cubesidestats.api.PlayerStatistics;
import de.iani.cubesidestats.api.StatisticKey;

public class PlayerStatisticsImplementation implements PlayerStatistics {
    private CubesideStatisticsImplementation stats;
    private final UUID playerId;
    private int databaseId;
    private Calendar calender = Calendar.getInstance();

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
        increaseScoreInMonth(key, amount, getCurrentMonthKey());
    }

    @Override
    public void setScore(StatisticKey key, int value) {
        setScoreInMonth(key, value, getCurrentMonthKey());
    }

    @Override
    public void maxScore(StatisticKey key, int value) {
        maxScore(key, value, null);
    }

    @Override
    public void maxScore(StatisticKey key, int value, Callback<Boolean> updatedCallback) {
        maxScoreInMonth(key, value, getCurrentMonthKey(), updatedCallback);
    }

    @Override
    public void minScore(StatisticKey key, int value) {
        minScore(key, value, null);
    }

    @Override
    public void minScore(StatisticKey key, int value, Callback<Boolean> updatedCallback) {
        minScoreInMonth(key, value, getCurrentMonthKey(), updatedCallback);
    }

    @Override
    public void getScore(StatisticKey key, Callback<Integer> scoreCallback) {
        getScoreInMonth(key, -1, scoreCallback);
    }

    @Override
    public void getPosition(StatisticKey key, Callback<Integer> positionCallback) {
        getPositionInMonth(key, -1, positionCallback);
    }

    @Override
    public void getScoreThisMonth(StatisticKey key, Callback<Integer> scoreCallback) {
        getScoreInMonth(key, getCurrentMonthKey(), scoreCallback);
    }

    @Override
    public void getPositionThisMonth(StatisticKey key, Callback<Integer> positionCallback) {
        getPositionInMonth(key, getCurrentMonthKey(), positionCallback);
    }

    private void minScoreInMonth(StatisticKey key, int value, int month, Callback<Boolean> updatedCallback) {
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
                    boolean result = database.minScore(databaseId, (StatisticKeyImplementation) key, month, value);
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

    private void maxScoreInMonth(StatisticKey key, int value, int month, Callback<Boolean> updatedCallback) {
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
                    boolean result = database.maxScore(databaseId, (StatisticKeyImplementation) key, month, value);
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

    private void setScoreInMonth(StatisticKey key, int value, int month) {
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
                    database.setScore(databaseId, (StatisticKeyImplementation) key, month, value);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set score for " + playerId, e);
                }
            }
        });
    }

    private void increaseScoreInMonth(StatisticKey key, int amount, int month) {
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
                    database.increaseScore(databaseId, (StatisticKeyImplementation) key, month, amount);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not increase score for " + playerId, e);
                }
            }
        });
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

    private int getCurrentMonthKey() {
        calender.setTimeInMillis(System.currentTimeMillis());
        return calender.get(Calendar.YEAR) * 100 + calender.get(Calendar.MONTH) + 1;
    }

    @Override
    public void grantAchivement(AchivementKey key) {
        // TODO Auto-generated method stub

    }

    @Override
    public void revokeAchivement(AchivementKey key) {
        // TODO Auto-generated method stub

    }

    @Override
    public void hasAchivement(AchivementKey key, Callback<Boolean> achivementCallback) {
        // TODO Auto-generated method stub

    }

}
