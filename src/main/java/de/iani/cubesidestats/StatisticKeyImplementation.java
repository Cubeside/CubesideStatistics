package de.iani.cubesidestats;

import de.iani.cubesidestats.api.StatisticKey;

public class StatisticKeyImplementation implements StatisticKey {

    private final int id;
    private final String name;

    public StatisticKeyImplementation(int id, String name, String properties) {
        this.id = id;
        this.name = name;

    }

    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setDisplayName(String name) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getDisplayName() {
        // TODO Auto-generated method stub

    }

    @Override
    public void setIsMonthlyStats(boolean monthly) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isMonthlyStats() {
        // TODO Auto-generated method stub
        return false;
    }

    public String getSerializedProperties() {
        // TODO Auto-generated method stub
        return "";
    }

    public void copyPropertiesFrom(StatisticKeyImplementation e) {
        // TODO Auto-generated method stub

    }

}
