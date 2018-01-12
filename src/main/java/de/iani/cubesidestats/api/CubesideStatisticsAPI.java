package de.iani.cubesidestats.api;

import java.util.UUID;

public interface CubesideStatisticsAPI {
    public PlayerStatistics getStatistics(UUID owner);

    public StatisticKey getStatisticKey(String id);

    public StatisticKey getStatisticKey(String id, boolean create);

    public boolean hasStatisticKey(String id);

    // public AchivementKey getAchivementKey(String id);

    // public boolean hasAchivementKey(String id);
}
