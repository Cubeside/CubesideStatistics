package de.iani.cubesidestats.schedular;

import de.iani.cubesidestats.CubesideStatistics;
import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.entity.Entity;

import java.util.function.Consumer;

public class FoliaScheduler implements Scheduler {

    private final CubesideStatistics plugin;

    public FoliaScheduler(CubesideStatistics plugin) {
        this.plugin = plugin;
    }

    @Override
    public void runAsync(Runnable task) {
        Bukkit.getServer().getAsyncScheduler().runNow(this.plugin, scheduledTask -> task.run());
    }

    @Override
    public void run(Runnable task) {
        Bukkit.getServer().getGlobalRegionScheduler().run(this.plugin, scheduledTask -> task.run());
    }

    @Override
    public CancellableTask runGlobalDelayed(Runnable task, long delay) {
        return Bukkit.getServer().getGlobalRegionScheduler().runDelayed(this.plugin, scheduledTask -> task.run(), delay)::cancel;
    }

    @Override
    public void runLocalDelayed(Location location, Runnable task, long delay) {
        Bukkit.getServer().getRegionScheduler().runDelayed(this.plugin, location, scheduledTask -> task.run(), delay);
    }

    @Override
    public CancellableTask runLocalAtFixedRate(Location location, Runnable task, long delay, long period) {
        return Bukkit.getServer().getRegionScheduler().runAtFixedRate(this.plugin, location, scheduledTask -> task.run(), delay, period)::cancel;
    }

    @Override
    public void runLocalAtFixedRate(Location location, Consumer<CancellableTask> taskConsumer, long delay, long period) {
        Bukkit.getServer().getRegionScheduler().runAtFixedRate(this.plugin, location, scheduledTask -> taskConsumer.accept(scheduledTask::cancel), delay, period);
    }

    @Override
    public CancellableTask runGlobalAtFixedRate(Runnable task, long delay, long period) {
        return Bukkit.getServer().getGlobalRegionScheduler().runAtFixedRate(this.plugin, scheduledTask -> task.run(), delay, period)::cancel;
    }

    @Override
    public void runGlobalAtFixedRate(Consumer<CancellableTask> taskConsumer, long delay, long period) {
        Bukkit.getServer().getGlobalRegionScheduler().runAtFixedRate(this.plugin, scheduledTask -> taskConsumer.accept(scheduledTask::cancel), delay, period);
    }

    @Override
    public CancellableTask runOnEntityAtFixedRate(Entity entity, Runnable task, long delay, long period) {
        ScheduledTask createdTask = entity.getScheduler().runAtFixedRate(this.plugin, scheduledTask -> task.run(), null, delay, period);
        return createdTask == null ? null : createdTask::cancel;
    }

    @Override
    public void runOnEntityAtFixedRate(Entity entity, Consumer<CancellableTask> taskConsumer, long delay, long period) {
        entity.getScheduler().runAtFixedRate(this.plugin, scheduledTask -> taskConsumer.accept(scheduledTask::cancel), null, delay, period);
    }

    @Override
    public void runDelayedOnEntity(Entity entity, Runnable task, long delay) {
        entity.getScheduler().runDelayed(this.plugin, scheduledTask -> task.run(), null, delay);
    }
}
