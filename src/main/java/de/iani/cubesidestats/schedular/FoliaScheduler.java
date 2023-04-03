package de.iani.cubesidestats.schedular;

import de.iani.cubesidestats.CubesideStatistics;
import org.bukkit.Bukkit;


public class FoliaScheduler implements Scheduler {

    private final CubesideStatistics plugin;

    public FoliaScheduler(CubesideStatistics plugin) {
        this.plugin = plugin;
    }

    @Override
    public void run(Runnable task) {
        Bukkit.getServer().getGlobalRegionScheduler().run(this.plugin, scheduledTask -> task.run());
    }
}
