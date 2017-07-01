package de.iani.cubesidestats.api;

public interface AchivementKey {
    public void setDisplayName(String name);

    public void getDisplayName();

    public void setSupersedes(AchivementKey other);

    public AchivementKey getSuperseded();

    public void setAutoGrant(StatisticKey key, int requiredValue);
}
