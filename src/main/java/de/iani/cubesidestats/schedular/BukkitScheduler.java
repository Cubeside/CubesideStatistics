package de.iani.cubesidestats.schedular;

import de.iani.cubesidestats.CubesideStatistics;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.entity.Entity;

import java.util.function.Consumer;

public class BukkitScheduler implements Scheduler {

    private final CubesideStatistics plugin;

    public BukkitScheduler(CubesideStatistics plugin) {
        this.plugin = plugin;
    }

    @Override
    public void runAsync(Runnable task) {
        Bukkit.getScheduler().runTaskAsynchronously(this.plugin, task);
    }

    @Override
    public void run(Runnable task) {
        Bukkit.getScheduler().runTask(this.plugin, task);
    }

    @Override
    public CancellableTask runGlobalDelayed(Runnable task, long delay) {
        return Bukkit.getScheduler().runTaskLater(this.plugin, task, delay)::cancel;
    }

    @Override
    public void runLocalDelayed(Location location, Runnable task, long delay) {
        runGlobalDelayed(task, delay);
    }

    @Override
    public CancellableTask runLocalAtFixedRate(Location location, Runnable task, long delay, long period) {
        return runGlobalAtFixedRate(task, delay, period);
    }

    @Override
    public void runLocalAtFixedRate(Location location, Consumer<CancellableTask> taskConsumer, long delay, long period) {
        runGlobalAtFixedRate(taskConsumer, delay, period);
    }

    @Override
    public CancellableTask runGlobalAtFixedRate(Runnable task, long delay, long period) {
        return Bukkit.getScheduler().runTaskTimer(this.plugin, task, delay, period)::cancel;
    }

    @Override
    public void runGlobalAtFixedRate(Consumer<CancellableTask> taskConsumer, long delay, long period) {
        Bukkit.getScheduler().runTaskTimer(this.plugin, bukkitTask -> taskConsumer.accept(bukkitTask::cancel), delay, period);
    }

    @Override
    public CancellableTask runOnEntityAtFixedRate(Entity entity, Runnable task, long delay, long period) {
        return runGlobalAtFixedRate(task, delay, period);
    }

    @Override
    public void runOnEntityAtFixedRate(Entity entity, Consumer<CancellableTask> taskConsumer, long delay, long period) {
        runLocalAtFixedRate(null, taskConsumer, delay, period);
    }

    @Override
    public void runDelayedOnEntity(Entity entity, Runnable task, long delay) {
        Bukkit.getScheduler().scheduleSyncDelayedTask(this.plugin, task, delay);
    }
}
