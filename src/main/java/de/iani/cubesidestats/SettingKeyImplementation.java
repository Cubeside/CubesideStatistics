package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;

import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.SettingKey;
import de.iani.cubesidestats.api.SettingValueSpec;

public class SettingKeyImplementation implements SettingKey {

    private final int id;
    private final String name;
    private final CubesideStatisticsImplementation stats;

    private String displayName;
    private int def;
    private SettingValueSpec valueSpec;

    public SettingKeyImplementation(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        this.id = id;
        this.name = name;
        this.stats = impl;
        this.def = 0;
        this.valueSpec = SettingValueSpec.unrestricted();

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
        try {
            valueSpec = loadValueSpec(conf);
        } catch (IllegalArgumentException e) {
            impl.getPlugin().getLogger().log(Level.SEVERE, "Could not load value specification for settings key " + name + " (" + id + ")", e);
            valueSpec = SettingValueSpec.unrestricted();
        }
        if (!valueSpec.isAllowed(def)) {
            impl.getPlugin().getLogger().warning("Default value " + def + " for settings key " + name + " (" + id + ") is outside its allowed range; using " + valueSpec.getMinimum());
            def = valueSpec.getMinimum();
        }
    }

    public String getSerializedProperties() {
        YamlConfiguration conf = new YamlConfiguration();
        conf.set("displayName", displayName);
        conf.set("default", def);
        conf.set("valueRange.min", valueSpec.getMinimum());
        conf.set("valueRange.max", valueSpec.getMaximum());
        Map<String, String> serializedMeanings = new LinkedHashMap<>();
        for (Map.Entry<Integer, String> entry : valueSpec.getMeanings().entrySet()) {
            serializedMeanings.put(Integer.toString(entry.getKey()), entry.getValue());
        }
        conf.set("valueMeanings", serializedMeanings);
        return conf.saveToString();
    }

    private static SettingValueSpec loadValueSpec(YamlConfiguration conf) {
        ConfigurationSection range = conf.getConfigurationSection("valueRange");
        int minimum = range == null ? Integer.MIN_VALUE : range.getInt("min", Integer.MIN_VALUE);
        int maximum = range == null ? Integer.MAX_VALUE : range.getInt("max", Integer.MAX_VALUE);

        Map<Integer, String> meanings = new LinkedHashMap<>();
        ConfigurationSection meaningsSection = conf.getConfigurationSection("valueMeanings");
        if (meaningsSection != null) {
            for (String key : meaningsSection.getKeys(false)) {
                int value;
                try {
                    value = Integer.parseInt(key);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid value meaning key: " + key, e);
                }
                String meaning = meaningsSection.getString(key);
                meanings.put(value, meaning);
            }
        }
        return SettingValueSpec.of(minimum, maximum, meanings);
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
        if (!valueSpec.isAllowed(def)) {
            throw new IllegalArgumentException("The default value " + def + " is outside the allowed range");
        }
        if (this.def != def) {
            this.def = def;
            save();
        }
    }

    @Override
    public int getDefault() {
        return def;
    }

    @Override
    public SettingValueSpec getValueSpec() {
        return valueSpec;
    }

    @Override
    public void setValueSpec(SettingValueSpec valueSpec) {
        Objects.requireNonNull(valueSpec, "valueSpec");
        if (!valueSpec.isAllowed(def)) {
            throw new IllegalArgumentException("The default value " + def + " is outside the new allowed range");
        }
        if (!this.valueSpec.equals(valueSpec)) {
            this.valueSpec = valueSpec;
            save();
        }
    }

    public void copyPropertiesFrom(SettingKeyImplementation e) {
        displayName = e.displayName;
        def = e.def;
        valueSpec = e.valueSpec;
    }
}
