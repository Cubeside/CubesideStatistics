package de.iani.cubesidestats;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.Callback;
import de.iani.cubesidestats.api.PlayerWithScore;
import de.iani.cubesidestats.api.StatisticKey;
import de.iani.cubesidestats.api.TimeFrame;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class StatisticKeyImplementation extends StatisticKeyImplementationBase implements StatisticKey {

    public StatisticKeyImplementation(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        super(id, name, properties, impl);
    }

    @Override
    protected void save() {
        StatisticKeyImplementation clone = new StatisticKeyImplementation(id, name, null, stats);
        clone.copyPropertiesFrom(this);

        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.updateStatisticKey(clone);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not save statistic key " + name, e);
                }
            }
        });
    }

    @Override
    public void getTop(int count, TimeFrame timeFrame, Callback<List<PlayerWithScore>> resultCallback) {
        boolean monthly = timeFrame == TimeFrame.MONTH;
        if (monthly && !isMonthlyStats()) {
            throw new IllegalArgumentException("There are no monthly stats for this key");
        }
        boolean daily = timeFrame == TimeFrame.DAY;
        if (daily && !isDailyStats()) {
            throw new IllegalArgumentException("There are no daily stats for this key");
        }
        if (resultCallback == null) {
            throw new NullPointerException("scoreCallback");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        int timekey = -1;
        if (monthly) {
            timekey = stats.getCurrentMonthKey();
        } else if (daily) {
            timekey = stats.getCurrentDayKey();
        }
        final int timekey2 = timekey;
        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    List<InternalPlayerWithScore> score = database.getTop(StatisticKeyImplementation.this, count, timekey2);
                    stats.getPlugin().getServer().getScheduler().runTask(stats.getPlugin(), new Runnable() {
                        @Override
                        public void run() {
                            ArrayList<PlayerWithScore> rv = new ArrayList<>();
                            for (InternalPlayerWithScore ip : score) {
                                rv.add(new PlayerWithScore(stats.getStatistics(ip.getPlayer()), ip.getScore(), ip.getPosition()));
                            }
                            resultCallback.call(rv);
                        }
                    });
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not get top scores for " + name, e);
                }
            }
        });
    }
}
