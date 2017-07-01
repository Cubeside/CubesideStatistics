package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.logging.Level;

import org.bukkit.plugin.ServicePriority;
import org.bukkit.plugin.java.JavaPlugin;

import de.iani.cubesidestats.api.CubesideStatisticsAPI;

public class CubesideStatistics extends JavaPlugin {
    private CubesideStatisticsImplementation impl;

    @Override
    public void onEnable() {
        try {
            impl = new CubesideStatisticsImplementation(this);
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "Could not connect to database: " + e.getMessage());
            return;
        }
        getServer().getServicesManager().register(CubesideStatisticsAPI.class, impl, this, ServicePriority.Normal);
    }

    @Override
    public void onDisable() {
        impl.shutdown();
    }
}
