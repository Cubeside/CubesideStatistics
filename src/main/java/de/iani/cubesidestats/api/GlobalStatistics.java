package de.iani.cubesidestats.api;

public interface GlobalStatistics {
    public void increaseValue(GlobalStatisticKey key, int amount);

    public void decreaseValue(GlobalStatisticKey key, int amount);

    public void setValue(GlobalStatisticKey key, int value);

    public void maxValue(GlobalStatisticKey key, int value);

    public void maxValue(GlobalStatisticKey key, int value, Callback<Boolean> updatedCallback);

    public void minValue(GlobalStatisticKey key, int value);

    public void minValue(GlobalStatisticKey key, int value, Callback<Boolean> updatedCallback);

    public void getValue(GlobalStatisticKey key, TimeFrame timeFrame, Callback<Integer> scoreCallback);
}
