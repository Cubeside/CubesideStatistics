package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.Objects;
import java.util.logging.Level;

import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.SettingKey;

public class SettingKeyImplementation implements SettingKey {

    private final int id;
    private final String name;
    private final CubesideStatisticsImplementation stats;

    private String displayName;
    private int def;

    public SettingKeyImplementation(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        this.id = id;
        this.name = name;
        this.stats = impl;
        this.def = 0;

        YamlConfiguration conf = new YamlConfiguration();
        if (properties != null) {
            try {
                conf.loadFromString(properties);
            } catch (InvalidConfigurationException e) {
                impl.getPlugin().getLogger().log(Level.SEVERE, "Could not load properties for settings key " + name + " (" + id + ")", e);
            }
        }
        displayName = conf.getString("displayName");
        def = conf.getInt("default");
    }

    public String getSerializedProperties() {
        YamlConfiguration conf = new YamlConfiguration();
        conf.set("displayName", displayName);
        conf.set("default", def);
        return conf.saveToString();
    }

    private void save() {
        SettingKeyImplementation clone = new SettingKeyImplementation(id, name, null, stats);
        clone.copyPropertiesFrom(this);

        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.updateSettingKey(clone);
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
    public void setDefault(int def) {
        if (this.def != def) {
            this.def = def;
            save();
        }
    }

    @Override
    public int getDefault() {
        return def;
    }

    public void copyPropertiesFrom(SettingKeyImplementation e) {
        displayName = e.displayName;
        def = e.def;
    }
}
