package de.iani.cubesidestats.api;

import com.google.common.base.Preconditions;
import java.util.Calendar;
import java.util.Objects;

public final class PlayerStatisticsQueryKey implements StatisticsQueryKey {
    public enum QueryType {
        SCORE,
        POSITION_MAX,
        POSITION_MIN,
        POSITION_MAX_TOTAL_ORDER,
        POSITION_MIN_TOTAL_ORDER
    }

    private final PlayerStatistics player;
    private final QueryType type;
    private final StatisticKey key;
    private final TimeFrame timeFrame;
    private final Calendar time;

    public PlayerStatisticsQueryKey(PlayerStatistics player, StatisticKey key) {
        this(player, key, QueryType.SCORE, TimeFrame.ALL_TIME, null);
    }

    public PlayerStatisticsQueryKey(PlayerStatistics player, StatisticKey key, QueryType type) {
        this(player, key, type, TimeFrame.ALL_TIME, null);
    }

    public PlayerStatisticsQueryKey(PlayerStatistics player, StatisticKey key, QueryType type, TimeFrame timeFrame) {
        this(player, key, type, timeFrame, null);
    }

    public PlayerStatisticsQueryKey(PlayerStatistics player, StatisticKey key, QueryType type, TimeFrame timeFrame, Calendar time) {
        Preconditions.checkNotNull(player, "player");
        Preconditions.checkNotNull(key, "key");
        Preconditions.checkNotNull(type, "type");
        Preconditions.checkNotNull(timeFrame, "timeFrame");
        this.player = player;
        this.key = key;
        this.type = type;
        this.timeFrame = timeFrame;
        this.time = time == null ? null : (Calendar) time.clone();
    }

    public PlayerStatistics getPlayer() {
        return player;
    }

    public StatisticKey getKey() {
        return key;
    }

    public QueryType getType() {
        return type;
    }

    public TimeFrame getTimeFrame() {
        return timeFrame;
    }

    public Calendar getTime() {
        return time == null ? null : (Calendar) time.clone();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != PlayerStatisticsQueryKey.class) {
            return false;
        }
        PlayerStatisticsQueryKey other = (PlayerStatisticsQueryKey) obj;
        return player.equals(other.player) && type == other.type && key == other.key && timeFrame == other.timeFrame && Objects.equals(time, other.time);
    }

    @Override
    public int hashCode() {
        return (((((player.hashCode() * 23 + key.hashCode()) * 7) + timeFrame.hashCode()) * 23) + type.hashCode()) * 31 + Objects.hashCode(time);
    }
}
