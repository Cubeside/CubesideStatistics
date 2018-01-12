package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.Objects;
import java.util.logging.Level;

import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.StatisticKey;

public class StatisticKeyImplementation implements StatisticKey {

    private final int id;
    private final String name;
    private final CubesideStatisticsImplementation impl;

    private String displayName;
    private boolean isMonthly;

    public StatisticKeyImplementation(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        this.id = id;
        this.name = name;
        this.impl = impl;

        YamlConfiguration conf = new YamlConfiguration();
        if (properties != null) {
            try {
                conf.loadFromString(properties);
            } catch (InvalidConfigurationException e) {
                impl.getPlugin().getLogger().log(Level.SEVERE, "Could not load properties for key " + name + " (" + id + ")", e);
            }
        }
        displayName = conf.getString("displayName");
        isMonthly = conf.getBoolean("isMonthly");
    }

    public String getSerializedProperties() {
        YamlConfiguration conf = new YamlConfiguration();
        conf.set("displayName", displayName);
        conf.set("isMonthly", isMonthly);
        return conf.saveToString();
    }

    private void save() {
        StatisticKeyImplementation clone = new StatisticKeyImplementation(id, name, null, impl);
        clone.copyPropertiesFrom(this);

        impl.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.updateStatisticKey(clone);
                } catch (SQLException e) {
                    impl.getPlugin().getLogger().log(Level.SEVERE, "Could not save statistic key " + name, e);
                }
            }
        });
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
        if (!Objects.equals(this.name, name)) {
            this.displayName = name;
            save();
        }
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public void setIsMonthlyStats(boolean monthly) {
        if (this.isMonthly != monthly) {
            this.isMonthly = monthly;
            save();
        }
    }

    @Override
    public boolean isMonthlyStats() {
        return isMonthly;
    }

    public void copyPropertiesFrom(StatisticKeyImplementation e) {
        displayName = e.displayName;
        isMonthly = e.isMonthly;
    }
}
