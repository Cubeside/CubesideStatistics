package de.iani.cubesidestats.api;

import java.util.Collection;
import java.util.UUID;

public interface CubesideStatisticsAPI {
    /**
     * Gets the statistics for a specific player
     *
     * @param owner
     *            the player who has statistics
     * @return the statistics, this will never be null
     */
    public PlayerStatistics getStatistics(UUID owner);

    /**
     * Gets an existing statistics key or creates a new one.
     *
     * @param id
     *            the id of the statistics key
     * @return the statistics key
     */
    public StatisticKey getStatisticKey(String id);

    /**
     * Gets an existing statistics key or creates a new one if it does not exist and create is true
     *
     * @param id
     *            the id of the statistics key
     * @param create
     *            true if the key should be created if it does not exist
     * @return the statistics key, this will never be null if create is true
     */
    public StatisticKey getStatisticKey(String id, boolean create);

    /**
     * Gets all existing statistic keys
     *
     * @return a collection containing all statistic keys
     */
    public Collection<? extends StatisticKey> getAllStatisticKeys();

    /**
     * Checks if a statistics key exists
     *
     * @param id
     *            the id of the statistics key
     * @return true if the key exists
     */
    public boolean hasStatisticKey(String id);

    public AchivementKey getAchivementKey(String id);

    public AchivementKey getAchivementKey(String id, boolean create);

    public Collection<? extends AchivementKey> getAllAchivementKeys();

    public boolean hasAchivementKey(String id);

    /**
     * Gets the player count manager that allows to set current player counts for different games
     *
     * @return the player count manger
     */
    public GamePlayerCount getGamePlayerCount();
}
