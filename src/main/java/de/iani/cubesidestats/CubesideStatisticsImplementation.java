package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.UUID;
import java.util.logging.Level;

import org.bukkit.entity.Player;

import de.iani.cubesidestats.api.AchivementKey;
import de.iani.cubesidestats.api.CubesideStatisticsAPI;
import de.iani.cubesidestats.api.PlayerStatistics;
import de.iani.cubesidestats.api.StatisticKey;

public class CubesideStatisticsImplementation implements CubesideStatisticsAPI {
    private HashMap<UUID, PlayerStatisticsImplementation> onlinePlayers;
    private StatisticsDatabase database;
    private CubesideStatistics plugin;
    private WorkerThread workerThread;

    public CubesideStatisticsImplementation(CubesideStatistics plugin) throws SQLException {
        this.plugin = plugin;
        database = new StatisticsDatabase(plugin, new SQLConfig(plugin.getConfig().getConfigurationSection("database")));

        onlinePlayers = new HashMap<UUID, PlayerStatisticsImplementation>();
        plugin.getServer().getPluginManager().registerEvents(new PlayerListener(this), plugin);

        workerThread = new WorkerThread();
        workerThread.start();
    }

    public void shutdown() {
        if (workerThread != null) {
            workerThread.shutdown();
        }
    }

    public WorkerThread getWorkerThread() {
        return workerThread;
    }

    public CubesideStatistics getPlugin() {
        return plugin;
    }

    @Override
    public PlayerStatistics getStatistics(UUID owner) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StatisticKey getStatisticKey(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasStatisticKey(String id) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public AchivementKey getAchivementKey(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasAchivementKey(String id) {
        // TODO Auto-generated method stub
        return false;
    }

    public void playerJoined(Player player) {
        onlinePlayers.put(player.getUniqueId(), new PlayerStatisticsImplementation(this, player.getUniqueId()));
    }

    public class WorkerThread extends Thread {

        private StatisticsDatabase database;

        private boolean stopping;

        private ArrayDeque<WorkEntry> work;

        public WorkerThread() {
            work = new ArrayDeque<WorkEntry>();
            this.database = CubesideStatisticsImplementation.this.database;
        }

        public void addWork(WorkEntry e) {
            synchronized (work) {
                work.addLast(e);
                work.notify();
            }
        }

        public void shutdown() {
            synchronized (work) {
                stopping = true;
                work.notify();
            }
            boolean interrupt = false;
            while (isAlive()) {
                try {
                    join();
                } catch (InterruptedException e) {
                    interrupt = true;
                }
            }
            if (interrupt) {
                Thread.currentThread().interrupt();
            }
            if (database != null) {
                database.disconnect();
                database = null;
            }
        }

        @Override
        public void run() {
            WorkEntry e;
            while (true) {
                synchronized (work) {
                    e = work.pollFirst();
                    if (e == null) {
                        if (stopping) {
                            return;
                        }
                        try {
                            work.wait();
                        } catch (InterruptedException e1) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
                if (e != null && database != null) {
                    try {
                        e.process(database);
                    } catch (Exception er) {
                        plugin.getLogger().log(Level.SEVERE, "Error in Statistics", er);
                    }
                }
            }
        }
    }

    public static interface WorkEntry {
        void process(StatisticsDatabase database);
    }
}
