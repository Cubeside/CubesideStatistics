package de.iani.cubesidestats.api.event;

import de.iani.cubesidestats.api.StatisticKey;
import java.util.UUID;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;

public class PlayerStatisticUpdatedEvent extends Event {
    private static final HandlerList handlers = new HandlerList();

    private final StatisticKey statistic;
    private final UUID player;
    private final Integer previousValueAllTime;
    private final int newValueAllTime;
    private final Integer previousValueMonthly;
    private final int newValueMonthly;
    private final Integer previousValueDaily;
    private final int newValueDaily;

    public PlayerStatisticUpdatedEvent(UUID who, StatisticKey statistic, Integer previousAlltime, int newAlltime, Integer previousMonthly, int newMonthly, Integer previousDaily, int newDaily) {
        this.player = who;
        this.statistic = statistic;
        this.previousValueAllTime = previousAlltime;
        this.newValueAllTime = newAlltime;
        this.previousValueMonthly = previousMonthly;
        this.newValueMonthly = newMonthly;
        this.previousValueDaily = previousDaily;
        this.newValueDaily = newDaily;
    }

    public StatisticKey getStatistic() {
        return statistic;
    }

    public UUID getPlayerUUID() {
        return player;
    }

    /**
     * @return true if this stats had a previous value for the player
     */
    public boolean hasPreviousValueAllTime() {
        return previousValueAllTime != null;
    }

    /**
     * check {@link #hasPreviousValueAllTime()} before calling this.
     *
     * @return the previous all time stats value for the player
     * @throws IllegalStateException
     *             if there as no previous value
     */
    public int getPreviousValueAllTime() {
        if (previousValueAllTime == null) {
            throw new IllegalStateException("There is no previous all time value for " + statistic.getName() + " and " + player);
        }
        return previousValueAllTime;
    }

    /**
     * @return the all time stats value for the player
     */
    public int getValueAllTime() {
        return newValueAllTime;
    }

    /**
     * check {@link de.iani.cubesidestats.api.StatistikKey#isMonthlyStats() getStatistic().isMonthlyStats()} before calling this.
     *
     * @return true if this stats had a previous monthly value for the player
     * @throws IllegalStateException
     *             if this stats is not recorded monthly
     */
    public boolean hasPreviousValueMonthly() {
        if (!statistic.isMonthlyStats()) {
            throw new IllegalStateException("Stats " + statistic.getName() + " is not recored monthly");
        }
        return previousValueMonthly != null;
    }

    /**
     * check {@link de.iani.cubesidestats.api.StatistikKey#isMonthlyStats() getStatistic().isMonthlyStats()} and {@link #hasPreviousValueMonthly()} before calling this.
     *
     * @return the previous monthly stats value for the player
     * @throws IllegalStateException
     *             if this stats is not recorded monthly or there as no previous value
     */
    public int getPreviousValueMonthly() {
        if (!statistic.isMonthlyStats()) {
            throw new IllegalStateException("Stats " + statistic.getName() + " is not recored monthly");
        }
        if (previousValueMonthly == null) {
            throw new IllegalStateException("There is no previous monthly value for " + statistic.getName() + " and " + player);
        }
        return previousValueMonthly;
    }

    /**
     * check {@link de.iani.cubesidestats.api.StatistikKey#isMonthlyStats() getStatistic().isMonthlyStats()} before calling this.
     *
     * @return the monthly stats value for the player
     * @throws IllegalStateException
     *             if this stats is not recorded monthly
     */
    public int getValueMonthly() {
        if (!statistic.isMonthlyStats()) {
            throw new IllegalStateException("Stats " + statistic.getName() + " is not recored monthly");
        }
        return newValueMonthly;
    }

    /**
     * check {@link de.iani.cubesidestats.api.StatistikKey#isDailyStats() getStatistic().isDailyStats()} before calling this.
     *
     * @return true if this stats had a previous daily value for the player
     * @throws IllegalStateException
     *             if this stats is not recorded daily
     */
    public boolean hasPreviousValueDaily() {
        if (!statistic.isDailyStats()) {
            throw new IllegalStateException("Stats " + statistic.getName() + " is not recored daily");
        }
        return previousValueDaily != null;
    }

    /**
     * check {@link de.iani.cubesidestats.api.StatistikKey#isDailyStats() getStatistic().isDailyStats()} and {@link #hasPreviousValueDaily()} before calling this.
     *
     * @return the previous daily stats value for the player
     * @throws IllegalStateException
     *             if this stats is not recorded daily or there as no previous value
     */
    public int getPreviousValueDaily() {
        if (!statistic.isDailyStats()) {
            throw new IllegalStateException("Stats " + statistic.getName() + " is not recored daily");
        }
        if (previousValueDaily == null) {
            throw new IllegalStateException("There is no previous daily value for " + statistic.getName() + " and " + player);
        }
        return previousValueDaily;
    }

    /**
     * check {@link de.iani.cubesidestats.api.StatistikKey#isDailyStats() getStatistic().isDailyStats()} before calling this.
     *
     * @return the daily stats value for the player
     * @throws IllegalStateException
     *             if this stats is not recorded daily
     */
    public int getValueDaily() {
        if (!statistic.isDailyStats()) {
            throw new IllegalStateException("Stats " + statistic.getName() + " is not recored daily");
        }
        return newValueDaily;
    }

    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }
}
