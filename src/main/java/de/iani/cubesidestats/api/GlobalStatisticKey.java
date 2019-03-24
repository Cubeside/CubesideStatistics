package de.iani.cubesidestats.api;

public interface GlobalStatisticKey {
    public String getName();

    public void setDisplayName(String name);

    public String getDisplayName();

    public void setIsMonthlyStats(boolean monthly);

    public boolean isMonthlyStats();

    public void setIsDailyStats(boolean daily);

    public boolean isDailyStats();
}
