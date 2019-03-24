package de.iani.cubesidestats;

import java.util.Objects;
import java.util.logging.Level;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

public abstract class StatisticKeyImplementationBase {

    protected final int id;
    protected final String name;
    protected final CubesideStatisticsImplementation stats;

    protected String displayName;
    protected boolean isMonthly;
    protected boolean isDaily;

    public StatisticKeyImplementationBase(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        this.id = id;
        this.name = name;
        this.stats = impl;

        YamlConfiguration conf = new YamlConfiguration();
        if (properties != null) {
            try {
                conf.loadFromString(properties);
            } catch (InvalidConfigurationException e) {
                impl.getPlugin().getLogger().log(Level.SEVERE, "Could not load properties for statistics key " + name + " (" + id + ")", e);
            }
        }
        displayName = conf.getString("displayName");
        isMonthly = conf.getBoolean("isMonthly");
        isDaily = conf.getBoolean("isDaily");
    }

    public String getSerializedProperties() {
        YamlConfiguration conf = new YamlConfiguration();
        conf.set("displayName", displayName);
        conf.set("isMonthly", isMonthly);
        conf.set("isDaily", isDaily);
        return conf.saveToString();
    }

    protected abstract void save();

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setDisplayName(String name) {
        if (!Objects.equals(this.displayName, name)) {
            this.displayName = name;
            save();
        }
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setIsMonthlyStats(boolean monthly) {
        if (this.isMonthly != monthly) {
            this.isMonthly = monthly;
            save();
        }
    }

    public boolean isMonthlyStats() {
        return isMonthly;
    }

    public void setIsDailyStats(boolean daily) {
        if (this.isDaily != daily) {
            this.isDaily = daily;
            save();
        }
    }

    public boolean isDailyStats() {
        return isDaily;
    }

    public void copyPropertiesFrom(StatisticKeyImplementationBase e) {
        displayName = e.displayName;
        isMonthly = e.isMonthly;
        isDaily = e.isDaily;
    }
}
