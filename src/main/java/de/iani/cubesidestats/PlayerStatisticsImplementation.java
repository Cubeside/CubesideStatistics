package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.UUID;
import java.util.logging.Level;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.AchivementKey;
import de.iani.cubesidestats.api.PlayerStatistics;
import de.iani.cubesidestats.api.StatisticKey;

public class PlayerStatisticsImplementation implements PlayerStatistics {
    private final UUID playerId;
    private int databaseId;

    public PlayerStatisticsImplementation(CubesideStatisticsImplementation stats, UUID player) {
        playerId = player;
        databaseId = -1;
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    databaseId = database.getOrCreatePlayerId(player);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not load database id for " + playerId, e);
                }
            }
        });
    }

    @Override
    public UUID getOwner() {
        return playerId;
    }

    @Override
    public void increaseScore(StatisticKey key, int amount) {
        // TODO Auto-generated method stub 123

    }

    @Override
    public void decreaseScore(StatisticKey key, int amount) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setScore(StatisticKey key, int value) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getScore(StatisticKey key, Callback<Integer> scoreCallback) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getPosition(StatisticKey key, Callback<Integer> positionCallback) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getScoreThisMonth(StatisticKey key, Callback<Integer> scoreCallback) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getPositionThisMonth(StatisticKey key, Callback<Integer> positionCallback) {
        // TODO Auto-generated method stub

    }

    @Override
    public void grantAchivement(AchivementKey key) {
        // TODO Auto-generated method stub

    }

    @Override
    public void revokeAchivement(AchivementKey key) {
        // TODO Auto-generated method stub

    }

    @Override
    public void hasAchivement(AchivementKey key, Callback<Boolean> achivementCallback) {
        // TODO Auto-generated method stub

    }

}
