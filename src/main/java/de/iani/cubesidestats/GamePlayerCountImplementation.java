package de.iani.cubesidestats;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.GamePlayerCount;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;

public class GamePlayerCountImplementation implements GamePlayerCount {

    private final CubesideStatisticsImplementation stats;
    private final HashMap<String, Integer> localPlayers;
    private HashMap<String, Integer> globalPlayers;
    private long lastUpdate;

    public GamePlayerCountImplementation(CubesideStatisticsImplementation stats) {
        this.stats = stats;
        this.localPlayers = new HashMap<>();
        this.globalPlayers = new HashMap<>();

        clearLocalPlayers();
        // stats.getPlugin().getServer().getScheduler().runTaskTimer(stats.getPlugin(), new Runnable() {
        // @Override
        // public void run() {
        // stats.getWorkerThread().addWork(new WorkEntry() {
        // @Override
        // public void process(StatisticsDatabase database) {
        // try {
        // HashMap<String, Integer> globalPlayersNew = database.getGamePlayers(stats.getServerId());
        // new BukkitRunnable() {
        // @Override
        // public void run() {
        // globalPlayers = globalPlayersNew;
        // }
        // }.runTask(stats.getPlugin());
        // } catch (SQLException e) {
        // stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get global player amount", e);
        // }
        // }
        // });
        // }
        // }, 200, 200);
        updateGlobalPlayerCount(null);
    }

    public void clearLocalPlayers() {
        localPlayers.clear();
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.deleteGamePlayers(stats.getServerId());
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not clear games playercounts", e);
                }
            }
        });
    }

    @Override
    public void addLocalPlayers(String game, int amount) {
        if (amount == 0) {
            return;
        }
        int newAmount = amount + getLocalPlayers(game);
        if (newAmount != 0) {
            localPlayers.put(game, newAmount);
        } else {
            localPlayers.remove(game);
        }

        updateDatabasePlayers(game, newAmount);
    }

    @Override
    public void subtractLocalPlayers(String game, int amount) {
        addLocalPlayers(game, -amount);
    }

    @Override
    public void setLocalPlayers(String game, int amount) {
        if (amount != 0) {
            localPlayers.put(game, amount);
        } else {
            localPlayers.remove(game);
        }
        updateDatabasePlayers(game, amount);
    }

    private void updateDatabasePlayers(String game, int newAmount) {
        if (stats.getPlugin().isEnabled()) {
            stats.getPlugin().getScheduler().run(() -> stats.getWorkerThread().addWork(database -> {
                try {
                    database.setGamePlayers(stats.getServerId(), game, newAmount);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not set player amount " + game, e);
                }
            }));
        }
    }

    @Override
    public void updateGlobalPlayerCount(Runnable callback) {
        if (lastUpdate + 10000 > System.currentTimeMillis()) {
            if (callback != null) {
                callback.run();
            }
            return;
        }
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                HashMap<String, Integer> globalPlayersNew = null;
                try {
                    globalPlayersNew = database.getGamePlayers(stats.getServerId());
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get global player amount", e);
                }
                HashMap<String, Integer> globalPlayersNew2 = globalPlayersNew;
                if (stats.getPlugin().isEnabled()) {
                    CubesideStatistics.getPlugin().getScheduler().run(() -> {
                        if (globalPlayersNew2 != null) {
                            globalPlayers = globalPlayersNew2;
                        }
                        lastUpdate = System.currentTimeMillis();
                        if (callback != null) {
                            callback.run();
                        }
                    });
                }
            }
        });
    }

    @Override
    public int getLocalPlayers(String game) {
        Integer local = localPlayers.get(game);
        return local == null ? 0 : local;
    }

    private int getGlobalPlayers(String game) {
        Integer global = globalPlayers.get(game);
        return global == null ? 0 : global;
    }

    @Override
    public int getPlayers(String game) {
        return getLocalPlayers(game) + getGlobalPlayers(game);
    }

}
