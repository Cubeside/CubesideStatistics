package de.iani.cubesidestats;

import de.iani.cubesidestats.api.CubesideStatisticsAPI;
import java.sql.SQLException;
import java.util.logging.Level;

import de.iani.cubesidestats.schedular.BukkitScheduler;
import de.iani.cubesidestats.schedular.FoliaScheduler;
import de.iani.cubesidestats.schedular.Scheduler;
import org.bukkit.plugin.ServicePriority;
import org.bukkit.plugin.java.JavaPlugin;

public class CubesideStatistics extends JavaPlugin {
    private CubesideStatisticsImplementation impl;
    private static CubesideStatistics plugin;
    private Scheduler scheduler;

    @Override
    public void onEnable() {
        saveDefaultConfig();

        plugin = this;
        try {
            Class.forName("io.papermc.paper.threadedregions.scheduler.ScheduledTask");
            getLogger().log(Level.INFO, "Folia found. Use Folia Scheduler");
            scheduler = new FoliaScheduler(this);
        } catch (Throwable ignored) {
            getLogger().log(Level.INFO, "Bukkit found. Use Bukkit Scheduler");
            scheduler = new BukkitScheduler(this);
        }

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

    public Scheduler getScheduler() {
        return scheduler;
    }

    public static CubesideStatistics getPlugin() {
        return plugin;
    }
}
