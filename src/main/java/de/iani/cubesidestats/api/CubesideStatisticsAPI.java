package de.iani.cubesidestats.api;

import java.util.Collection;
import java.util.Map;
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

    /**
     * Gets the global statistics
     *
     * @return the statistics, this will never be null
     */
    public GlobalStatistics getGlobalStatistics();

    /**
     * Gets an existing global statistics key or creates a new one.
     *
     * @param id
     *            the id of the global statistics key
     * @return the statistics key
     */
    public GlobalStatisticKey getGlobalStatisticKey(String id);

    /**
     * Gets an existing global statistics key or creates a new one if it does not exist and create is true
     *
     * @param id
     *            the id of the global statistics key
     * @param create
     *            true if the key should be created if it does not exist
     * @return the global statistics key, this will never be null if create is true
     */
    public GlobalStatisticKey getGlobalStatisticKey(String id, boolean create);

    /**
     * Gets all existing global statistic keys
     *
     * @return a collection containing all global statistic keys
     */
    public Collection<? extends GlobalStatisticKey> getAllGlobalStatisticKeys();

    /**
     * Checks if a global statistics key exists
     *
     * @param id
     *            the id of the global statistics key
     * @return true if the key exists
     */
    public boolean hasGlobalStatisticKey(String id);

    public AchivementKey getAchivementKey(String id);

    public AchivementKey getAchivementKey(String id, boolean create);

    public Collection<? extends AchivementKey> getAllAchivementKeys();

    public boolean hasAchivementKey(String id);

    /**
     * Gets an existing setting key or creates a new one.
     *
     * @param id
     *            the id of the setting key
     * @return the setting key
     */
    public SettingKey getSettingKey(String id);

    /**
     * Gets an existing setting key or creates a new one if it does not exist and create is true
     *
     * @param id
     *            the id of the setting key
     * @param create
     *            true if the key should be created if it does not exist
     * @return the setting key, this will never be null if create is true
     */
    public SettingKey getSettingKey(String id, boolean create);

    public Collection<? extends SettingKey> getAllSettingKeys();

    public boolean hasSettingKey(String id);

    /**
     * Gets the player count manager that allows to set current player counts for different games
     *
     * @return the player count manger
     */
    public GamePlayerCount getGamePlayerCount();

    /**
     * Querys multiple statistics values. They can be for different players and can also include global statistics.
     *
     * @param querys
     *            The querys to be executed
     * @param callback
     *            A callback that is called when all data is collected. This callback is called in the main thread.
     */
    public void queryStats(Collection<StatisticsQueryKey> querys, Callback<Map<StatisticsQueryKey, Integer>> callback);
}
