package de.iani.cubesidestats;

import com.google.common.base.Preconditions;
import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.Callback;
import de.iani.cubesidestats.api.Ordering;
import de.iani.cubesidestats.api.PlayerWithScore;
import de.iani.cubesidestats.api.PositionAlgorithm;
import de.iani.cubesidestats.api.StatisticKey;
import de.iani.cubesidestats.api.TimeFrame;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.logging.Level;

public class StatisticKeyImplementation extends StatisticKeyImplementationBase implements StatisticKey {

    public StatisticKeyImplementation(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        super(id, name, properties, impl);
    }

    @Override
    protected void save() {
        StatisticKeyImplementation clone = new StatisticKeyImplementation(id, name, null, stats);
        clone.copyPropertiesFrom(this);

        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.updateStatisticKey(clone);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not save statistic key " + name, e);
                }
            }
        });
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int count, TimeFrame timeFrame, Callback<List<PlayerWithScore>> resultCallback) {
        return getTop(0, count, Ordering.DESCENDING, timeFrame, resultCallback);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int count, TimeFrame timeFrame, Calendar time, Callback<List<PlayerWithScore>> resultCallback) {
        return getTop(0, count, Ordering.DESCENDING, timeFrame, time, resultCallback);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame) {
        return getTop(start, count, order, timeFrame, (Callback<List<PlayerWithScore>>) null);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time) {
        return getTop(start, count, order, timeFrame, time, (Callback<List<PlayerWithScore>>) null);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Callback<List<PlayerWithScore>> resultCallback) {
        return getTop(start, count, order, timeFrame, PositionAlgorithm.TOTAL_ORDER, resultCallback);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, Callback<List<PlayerWithScore>> resultCallback) {
        return getTop(start, count, order, timeFrame, time, PositionAlgorithm.TOTAL_ORDER, resultCallback);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, PositionAlgorithm positionAlgorithm) {
        return getTop(start, count, order, timeFrame, positionAlgorithm, (Callback<List<PlayerWithScore>>) null);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, PositionAlgorithm positionAlgorithm) {
        return getTop(start, count, order, timeFrame, time, positionAlgorithm, (Callback<List<PlayerWithScore>>) null);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, PositionAlgorithm positionAlgorithm, Callback<List<PlayerWithScore>> resultCallback) {
        return getTop(start, count, order, timeFrame, positionAlgorithm, order, resultCallback);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, PositionAlgorithm positionAlgorithm, Callback<List<PlayerWithScore>> resultCallback) {
        return getTop(start, count, order, timeFrame, time, positionAlgorithm, order, resultCallback);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, PositionAlgorithm positionAlgorithm, Ordering positionOrder) {
        return getTop(start, count, order, timeFrame, positionAlgorithm, positionOrder, (Callback<List<PlayerWithScore>>) null);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, PositionAlgorithm positionAlgorithm, Ordering positionOrder) {
        return getTop(start, count, order, timeFrame, time, positionAlgorithm, positionOrder, (Callback<List<PlayerWithScore>>) null);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, PositionAlgorithm positionAlgorithm, Ordering positionOrder, Callback<List<PlayerWithScore>> resultCallback) {
        int timekey = -1;
        if (timeFrame == TimeFrame.MONTH) {
            timekey = stats.getCurrentMonthKey();
        } else if (timeFrame == TimeFrame.DAY) {
            timekey = stats.getCurrentDayKey();
        }
        return getTop(start, count, order, timeFrame, timekey, positionAlgorithm, positionOrder, resultCallback);
    }

    @Override
    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, PositionAlgorithm positionAlgorithm, Ordering positionOrder, Callback<List<PlayerWithScore>> resultCallback) {
        int timekey = -1;
        if (timeFrame == TimeFrame.MONTH) {
            timekey = CubesideStatisticsImplementation.getMonthKey(time);
        } else if (timeFrame == TimeFrame.DAY) {
            timekey = CubesideStatisticsImplementation.getDayKey(time);
        }
        return getTop(start, count, order, timeFrame, timekey, positionAlgorithm, positionOrder, resultCallback);
    }

    private Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, int timeKey, PositionAlgorithm positionAlgorithm, Ordering positionOrder, Callback<List<PlayerWithScore>> resultCallback) {
        if (timeFrame == TimeFrame.MONTH && !isMonthlyStats()) {
            throw new IllegalArgumentException("There are no monthly stats for this key");
        }
        if (timeFrame == TimeFrame.DAY && !isDailyStats()) {
            throw new IllegalArgumentException("There are no daily stats for this key");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        Preconditions.checkNotNull(order, "order");
        Preconditions.checkNotNull(positionOrder, "positionOrder");
        Preconditions.checkNotNull(positionAlgorithm, "positionAlgorithm");

        CompletableFuture<List<PlayerWithScore>> future = new CompletableFuture<>();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    List<PlayerWithScore> scoreList = new ArrayList<>();
                    List<InternalPlayerWithScore> scoreInternal = database.getTop(StatisticKeyImplementation.this, start, count, order, timeKey, positionAlgorithm, positionOrder);
                    for (InternalPlayerWithScore ip : scoreInternal) {
                        scoreList.add(new PlayerWithScore(stats.getStatistics(ip.getPlayer()), ip.getScore(), ip.getPosition()));
                    }
                    List<PlayerWithScore> unmodifiableScoreList = Collections.unmodifiableList(scoreList);
                    future.complete(unmodifiableScoreList);
                    if (resultCallback != null && stats.getPlugin().isEnabled()) {
                        stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                            @Override
                            public void run() {
                                resultCallback.call(unmodifiableScoreList);
                            }
                        });
                    }
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get top scores for " + name, e);
                }
            }
        });
        return future;
    }

    @Override
    public Future<Integer> getEntriesCount(TimeFrame timeFrame) {
        boolean monthly = timeFrame == TimeFrame.MONTH;
        if (monthly && !isMonthlyStats()) {
            throw new IllegalArgumentException("There are no monthly stats for this key");
        }
        boolean daily = timeFrame == TimeFrame.DAY;
        if (daily && !isDailyStats()) {
            throw new IllegalArgumentException("There are no daily stats for this key");
        }
        int timekey = -1;
        if (monthly) {
            timekey = stats.getCurrentMonthKey();
        } else if (daily) {
            timekey = stats.getCurrentDayKey();
        }
        final int timekey2 = timekey;

        CompletableFuture<Integer> future = new CompletableFuture<>();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    int entries = database.getScoreEntries(StatisticKeyImplementation.this, timekey2);
                    future.complete(entries);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get top scores for " + name, e);
                }
            }
        });
        return future;
    }
}
