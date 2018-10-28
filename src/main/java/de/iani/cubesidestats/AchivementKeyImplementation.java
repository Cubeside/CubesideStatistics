package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.Objects;
import java.util.logging.Level;

import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

import com.google.common.base.Preconditions;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.AchivementKey;

public class AchivementKeyImplementation implements AchivementKey {

    private final int id;
    private final String name;
    private final CubesideStatisticsImplementation stats;

    private String displayName;
    private int maxLevel;

    public AchivementKeyImplementation(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        this.id = id;
        this.name = name;
        this.stats = impl;
        this.maxLevel = 1;

        YamlConfiguration conf = new YamlConfiguration();
        if (properties != null) {
            try {
                conf.loadFromString(properties);
            } catch (InvalidConfigurationException e) {
                impl.getPlugin().getLogger().log(Level.SEVERE, "Could not load properties for achivement key " + name + " (" + id + ")", e);
            }
        }
        displayName = conf.getString("displayName");
        maxLevel = Math.max(conf.getInt("maxLevel"), 1);
    }

    public String getSerializedProperties() {
        YamlConfiguration conf = new YamlConfiguration();
        conf.set("displayName", displayName);
        conf.set("maxLevel", maxLevel);
        return conf.saveToString();
    }

    private void save() {
        AchivementKeyImplementation clone = new AchivementKeyImplementation(id, name, null, stats);
        clone.copyPropertiesFrom(this);

        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.updateAchivementKey(clone);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not save achivement key " + name, e);
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
        if (!Objects.equals(this.displayName, name)) {
            this.displayName = name;
            save();
        }
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public void setMaxLevel(int level) {
        Preconditions.checkArgument(level >= 1, "level must be 1 or more");
        if (this.maxLevel != level) {
            this.maxLevel = level;
            save();
        }
    }

    @Override
    public int getMaxLevel() {
        return maxLevel;
    }

    public void copyPropertiesFrom(AchivementKeyImplementation e) {
        displayName = e.displayName;
        maxLevel = e.maxLevel;
    }
}
