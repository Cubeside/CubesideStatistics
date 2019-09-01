package de.iani.cubesidestats.api;

import com.google.common.base.Preconditions;

public final class GlobalStatisticsQueryKey implements StatisticsQueryKey {
    private final GlobalStatisticKey key;
    private final TimeFrame timeFrame;

    public GlobalStatisticsQueryKey(GlobalStatisticKey key) {
        this(key, TimeFrame.ALL_TIME);
    }

    public GlobalStatisticsQueryKey(GlobalStatisticKey key, TimeFrame timeFrame) {
        Preconditions.checkNotNull(key, "key");
        Preconditions.checkNotNull(timeFrame, "timeFrame");
        this.key = key;
        this.timeFrame = timeFrame;
    }

    public GlobalStatisticKey getKey() {
        return key;
    }

    public TimeFrame getTimeFrame() {
        return timeFrame;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != GlobalStatisticsQueryKey.class) {
            return false;
        }
        GlobalStatisticsQueryKey other = (GlobalStatisticsQueryKey) obj;
        return key == other.key && timeFrame == other.timeFrame;
    }

    @Override
    public int hashCode() {
        return (key.hashCode() * 23) + timeFrame.hashCode();
    }
}
