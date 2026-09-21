package de.iani.cubesidestats.api;

import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

public interface StatisticKey {
    public String getName();

    public void setDisplayName(String name);

    public String getDisplayName();

    public void setIsMonthlyStats(boolean monthly);

    public boolean isMonthlyStats();

    public void setIsDailyStats(boolean daily);

    public boolean isDailyStats();

    public Future<List<PlayerWithScore>> getTop(int count, TimeFrame timeFrame, Callback<List<PlayerWithScore>> resultCallback);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Callback<List<PlayerWithScore>> resultCallback);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, PositionAlgorithm positionAlgorithm, Callback<List<PlayerWithScore>> resultCallback);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, PositionAlgorithm positionAlgorithm);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, PositionAlgorithm positionAlgorithm, Ordering positionOrder, Callback<List<PlayerWithScore>> resultCallback);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, PositionAlgorithm positionAlgorithm, Ordering positionOrder);

    public Future<List<PlayerWithScore>> getTop(int count, TimeFrame timeFrame, Calendar time, Callback<List<PlayerWithScore>> resultCallback);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, Callback<List<PlayerWithScore>> resultCallback);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, PositionAlgorithm positionAlgorithm, Callback<List<PlayerWithScore>> resultCallback);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, PositionAlgorithm positionAlgorithm);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, PositionAlgorithm positionAlgorithm, Ordering positionOrder, Callback<List<PlayerWithScore>> resultCallback);

    public Future<List<PlayerWithScore>> getTop(int start, int count, Ordering order, TimeFrame timeFrame, Calendar time, PositionAlgorithm positionAlgorithm, Ordering positionOrder);

    public Future<Integer> getEntriesCount(TimeFrame timeFrame);

    /**
     * Finds players with a stored score matching the comparison. Players without a stored value are
     * deliberately not returned; callers can apply their own missing-value policy.
     */
    public Future<Set<UUID>> findPlayersByScore(ScoreComparison comparison, int value, TimeFrame timeFrame, Calendar time);

    public default Future<Integer> getEntriesCount(TimeFrame timeFrame, Calendar time) {
        return getEntriesCount(timeFrame);
    }

    public Future<Integer> transformAllScores(int multiplier, int addend);

    public Future<Integer> transformAllScores(int multiplier, int addend, Callback<Integer> resultCallback);

}
