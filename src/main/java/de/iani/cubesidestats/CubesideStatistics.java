package de.iani.cubesidestats;

import de.iani.cubesidestats.api.CubesideStatisticsAPI;
import de.iani.cubesidestats.api.conditions.HasSettingCondition;
import de.iani.cubesideutils.serialization.StringSerialization;
import java.sql.SQLException;
import java.util.logging.Level;
import org.bukkit.plugin.ServicePriority;
import org.bukkit.plugin.java.JavaPlugin;

public class CubesideStatistics extends JavaPlugin {
    private CubesideStatisticsImplementation impl;

    @Override
    public void onLoad() {
        StringSerialization.register(HasSettingCondition.SERIALIZATION_TYPE, HasSettingCondition::deserialize);
    }

    @Override
    public void onEnable() {
        saveDefaultConfig();
        SQLConfig sqlConfig = new SQLConfig(getConfig().getConfigurationSection("database"));
        try {
            impl = new CubesideStatisticsImplementation(this, sqlConfig);
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "Could not connect to database: " + e.getMessage(), e);
            return;
        }
        getServer().getServicesManager().register(CubesideStatisticsAPI.class, impl, this, ServicePriority.Normal);
    }

    @Override
    public void onDisable() {
        if (impl != null) {
            impl.shutdown();
        }
    }
}
