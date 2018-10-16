package de.iani.cubesidestats.api;

import java.util.UUID;

public interface PlayerStatistics {
    public UUID getOwner();

    public void increaseScore(StatisticKey key, int amount);

    public void decreaseScore(StatisticKey key, int amount);

    public void setScore(StatisticKey key, int value);

    public void maxScore(StatisticKey key, int value);

    public void maxScore(StatisticKey key, int value, Callback<Boolean> updatedCallback);

    public void minScore(StatisticKey key, int value);

    public void minScore(StatisticKey key, int value, Callback<Boolean> updatedCallback);

    public void getScore(StatisticKey key, TimeFrame timeFrame, Callback<Integer> scoreCallback);

    public void getPosition(StatisticKey key, TimeFrame timeFrame, Callback<Integer> positionCallback);

    public void grantAchivement(AchivementKey key);

    public void grantAchivement(AchivementKey key, int level);

    public void revokeAchivement(AchivementKey key);

    public void hasAchivement(AchivementKey key, Callback<Boolean> achivementCallback);

    public void getAchivementLevel(AchivementKey key, Callback<Integer> achivementCallback);

}
