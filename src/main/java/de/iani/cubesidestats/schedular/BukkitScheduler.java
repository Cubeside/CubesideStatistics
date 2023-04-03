package de.iani.cubesidestats.schedular;

import de.iani.cubesidestats.CubesideStatistics;
import org.bukkit.Bukkit;

public class BukkitScheduler implements Scheduler {

    private final CubesideStatistics plugin;

    public BukkitScheduler(CubesideStatistics plugin) {
        this.plugin = plugin;
    }

    @Override
    public void run(Runnable task) {
        Bukkit.getScheduler().runTask(this.plugin, task);
    }
}
