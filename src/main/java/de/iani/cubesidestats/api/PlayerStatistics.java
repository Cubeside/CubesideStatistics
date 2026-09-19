package de.iani.cubesidestats.api;

import java.util.UUID;

public interface PlayerStatistics {
    public UUID getOwner();

    public void increaseScore(StatisticKey key, int amount);

    public void decreaseScore(StatisticKey key, int amount);

    public void setScore(StatisticKey key, int value);

    public void deleteScore(StatisticKey key);

    public void maxScore(StatisticKey key, int value);

    public void maxScore(StatisticKey key, int value, Callback<Boolean> updatedCallback);

    public void minScore(StatisticKey key, int value);

    public void minScore(StatisticKey key, int value, Callback<Boolean> updatedCallback);

    public void getScore(StatisticKey key, TimeFrame timeFrame, Callback<Integer> scoreCallback);

    public void getPosition(StatisticKey key, TimeFrame timeFrame, Callback<Integer> positionCallback);

    public void grantAchivement(AchivementKey key);

    public void grantAchivement(AchivementKey key, Callback<Integer> updatedCallback);

    public void grantAchivement(AchivementKey key, int level);

    public void grantAchivement(AchivementKey key, int level, Callback<Integer> updatedCallback);

    public void revokeAchivement(AchivementKey key);

    public void revokeAchivement(AchivementKey key, Callback<Integer> updatedCallback);

    public void hasAchivement(AchivementKey key, Callback<Boolean> achivementCallback);

    public void getAchivementLevel(AchivementKey key, Callback<Integer> achivementCallback);

    /**
     * Returns whether this player's settings have been loaded from the database.
     *
     * <p>For a player logging in, this is guaranteed to return {@code true} after CubesideStatistics has handled
     * {@link org.bukkit.event.player.AsyncPlayerPreLoginEvent}. The settings are therefore available in subsequent
     * login events and when {@link org.bukkit.event.player.PlayerJoinEvent} is called. A database load failure
     * prevents the player from logging in.</p>
     *
     * @return {@code true} if the settings are available
     */
    public boolean areSettingsLoaded();

    public Integer getSettingValueIfLoaded(SettingKey setting);

    public int getSettingValueOrDefault(SettingKey setting);

    public void setSettingValue(SettingKey key, int value);
}
