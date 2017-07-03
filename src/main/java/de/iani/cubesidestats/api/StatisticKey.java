package de.iani.cubesidestats.api;

public interface StatisticKey {
    public String getName();

    public void setDisplayName(String name);

    public void getDisplayName();

    public void setIsMonthlyStats(boolean monthly);

    public boolean isMonthlyStats();
}
