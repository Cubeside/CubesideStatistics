package de.iani.cubesidestats.api;

import java.util.Collection;
import java.util.UUID;

import de.iani.cubesidestats.StatisticKeyImplementation;

public interface CubesideStatisticsAPI {
    public PlayerStatistics getStatistics(UUID owner);

    public StatisticKey getStatisticKey(String id);

    public StatisticKey getStatisticKey(String id, boolean create);

    public Collection<StatisticKeyImplementation> getAllStatisticKeys();

    public boolean hasStatisticKey(String id);

    // public AchivementKey getAchivementKey(String id);

    // public boolean hasAchivementKey(String id);

    public GamePlayerCount getGamePlayerCount();
}
