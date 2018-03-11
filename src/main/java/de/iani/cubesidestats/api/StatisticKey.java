package de.iani.cubesidestats.api;

import java.util.List;

public interface StatisticKey {
    public String getName();

    public void setDisplayName(String name);

    public String getDisplayName();

    public void setIsMonthlyStats(boolean monthly);

    public boolean isMonthlyStats();

    public void getTop(int count, boolean monthly, Callback<List<PlayerWithScore>> resultCallback);
}
