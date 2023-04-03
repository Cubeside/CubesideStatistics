package de.iani.cubesidestats.schedular;

import org.bukkit.Location;
import org.bukkit.entity.Entity;

import java.util.function.Consumer;

public interface Scheduler {

    void runAsync(Runnable task);

    void run(Runnable task);

    CancellableTask runGlobalDelayed(Runnable task, long delay);

    default void runGlobalDelayed(Runnable task) {
        runGlobalDelayed(task, 1);
    }

    void runLocalDelayed(Location location, Runnable task, long delay);

    default void runLocalDelayed(Runnable task) {
        runGlobalDelayed(task, 1);
    }

    CancellableTask runLocalAtFixedRate(Location location, Runnable task, long delay, long period);

    void runLocalAtFixedRate(Location location, Consumer<CancellableTask> taskConsumer, long delay, long period);

    CancellableTask runGlobalAtFixedRate(Runnable task, long delay, long period);

    void runGlobalAtFixedRate(Consumer<CancellableTask> taskConsumer, long delay, long period);

    CancellableTask runOnEntityAtFixedRate(Entity entity, Runnable task, long delay, long period);

    void runOnEntityAtFixedRate(Entity entity, Consumer<CancellableTask> taskConsumer, long delay, long period);

    void runDelayedOnEntity(Entity entity, Runnable task, long delay);
}
