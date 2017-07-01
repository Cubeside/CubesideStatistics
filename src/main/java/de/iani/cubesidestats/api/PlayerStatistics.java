package de.iani.cubesidestats.api;

import java.util.UUID;

import de.iani.cubesidestats.Callback;

public interface PlayerStatistics {
    public UUID getOwner();

    public void increaseScore(StatisticKey key, int amount);

    public void decreaseScore(StatisticKey key, int amount);

    public void setScore(StatisticKey key, int value);

    public void getScore(StatisticKey key, Callback<Integer> scoreCallback);

    public void getPosition(StatisticKey key, Callback<Integer> positionCallback);

    public void getScoreThisMonth(StatisticKey key, Callback<Integer> scoreCallback);

    public void getPositionThisMonth(StatisticKey key, Callback<Integer> positionCallback);

    public void grantAchivement(AchivementKey key);

    public void revokeAchivement(AchivementKey key);

    public void hasAchivement(AchivementKey key, Callback<Boolean> achivementCallback);
}
