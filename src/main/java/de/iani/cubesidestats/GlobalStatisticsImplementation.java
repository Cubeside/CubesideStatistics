package de.iani.cubesidestats;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.Callback;
import de.iani.cubesidestats.api.GlobalStatisticKey;
import de.iani.cubesidestats.api.GlobalStatistics;
import de.iani.cubesidestats.api.TimeFrame;
import java.sql.SQLException;
import java.util.logging.Level;

public class GlobalStatisticsImplementation implements GlobalStatistics {
    private CubesideStatisticsImplementation stats;

    public GlobalStatisticsImplementation(CubesideStatisticsImplementation stats) {
        if (stats == null) {
            throw new NullPointerException("stats");
        }
        this.stats = stats;
    }

    @Override
    public void decreaseValue(GlobalStatisticKey key, int amount) {
        increaseValue(key, -amount);
    }

    @Override
    public void increaseValue(GlobalStatisticKey key, int amount) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        final int month = stats.getCurrentMonthKey();
        final int daykey = stats.getCurrentDayKey();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.increaseGlobalStatsValue((GlobalStatisticKeyImplementation) key, month, daykey, amount);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not increase global value", e);
                }
            }
        });
    }

    @Override
    public void setValue(GlobalStatisticKey key, int value) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        final int month = stats.getCurrentMonthKey();
        final int daykey = stats.getCurrentDayKey();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.setGlobalStatsValue((GlobalStatisticKeyImplementation) key, month, daykey, value);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set global value", e);
                }
            }
        });
    }

    @Override
    public void maxValue(GlobalStatisticKey key, int value) {
        maxValue(key, value, null);
    }

    @Override
    public void maxValue(GlobalStatisticKey key, int value, Callback<Boolean> updatedCallback) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        final int month = stats.getCurrentMonthKey();
        final int daykey = stats.getCurrentDayKey();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    boolean result = database.maxGlobalStatsValue((GlobalStatisticKeyImplementation) key, month, daykey, value);
                    if (updatedCallback != null && stats.getPlugin().isEnabled()) {
                        stats.getPlugin().getScheduler().run(() -> updatedCallback.call(result));
                    }
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set global value", e);
                }
            }
        });
    }

    @Override
    public void minValue(GlobalStatisticKey key, int value) {
        minValue(key, value, null);
    }

    @Override
    public void minValue(GlobalStatisticKey key, int value, Callback<Boolean> updatedCallback) {
        if (!(key instanceof StatisticKeyImplementation)) {
            throw new IllegalArgumentException("key");
        }
        final int month = stats.getCurrentMonthKey();
        final int daykey = stats.getCurrentDayKey();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    boolean result = database.minGlobalStatsValue((GlobalStatisticKeyImplementation) key, month, daykey, value);
                    if (updatedCallback != null && stats.getPlugin().isEnabled()) {
                        stats.getPlugin().getScheduler().run(() -> updatedCallback.call(result));
                    }
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set global value", e);
                }
            }
        });
    }

    @Override
    public void getValue(GlobalStatisticKey key, TimeFrame timeFrame, Callback<Integer> scoreCallback) {
        getScoreInMonth(key, getMonthKey(timeFrame), scoreCallback);
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

    private void getScoreInMonth(GlobalStatisticKey key, int month, Callback<Integer> scoreCallback) {
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
                if (score != null && stats.getPlugin().isEnabled()) {
                    stats.getPlugin().getScheduler().run(() -> scoreCallback.call(score));
                }
            }
        });
    }

    protected Integer internalGetScoreInMonth(StatisticsDatabase database, GlobalStatisticKey key, int month) {
        try {
            return database.getGlobalStatsValue((GlobalStatisticKeyImplementation) key, month);
        } catch (SQLException e) {
            stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get global value", e);
        }
        return null;
    }
}
