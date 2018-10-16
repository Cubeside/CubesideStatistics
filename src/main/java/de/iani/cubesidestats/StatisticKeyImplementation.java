package de.iani.cubesidestats;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;

import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.Callback;
import de.iani.cubesidestats.api.PlayerWithScore;
import de.iani.cubesidestats.api.StatisticKey;
import de.iani.cubesidestats.api.TimeFrame;

public class StatisticKeyImplementation implements StatisticKey {

    private final int id;
    private final String name;
    private final CubesideStatisticsImplementation stats;

    private String displayName;
    private boolean isMonthly;
    private boolean isDaily;

    public StatisticKeyImplementation(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        this.id = id;
        this.name = name;
        this.stats = impl;

        YamlConfiguration conf = new YamlConfiguration();
        if (properties != null) {
            try {
                conf.loadFromString(properties);
            } catch (InvalidConfigurationException e) {
                impl.getPlugin().getLogger().log(Level.SEVERE, "Could not load properties for statistics key " + name + " (" + id + ")", e);
            }
        }
        displayName = conf.getString("displayName");
        isMonthly = conf.getBoolean("isMonthly");
        isDaily = conf.getBoolean("isDaily");
    }

    public String getSerializedProperties() {
        YamlConfiguration conf = new YamlConfiguration();
        conf.set("displayName", displayName);
        conf.set("isMonthly", isMonthly);
        conf.set("isDaily", isDaily);
        return conf.saveToString();
    }

    private void save() {
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

    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setDisplayName(String name) {
        if (!Objects.equals(this.name, name)) {
            this.displayName = name;
            save();
        }
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public void setIsMonthlyStats(boolean monthly) {
        if (this.isMonthly != monthly) {
            this.isMonthly = monthly;
            save();
        }
    }

    @Override
    public boolean isMonthlyStats() {
        return isMonthly;
    }

    @Override
    public void setIsDailyStats(boolean daily) {
        if (this.isDaily != daily) {
            this.isDaily = daily;
            save();
        }
    }

    @Override
    public boolean isDailyStats() {
        return isDaily;
    }

    public void copyPropertiesFrom(StatisticKeyImplementation e) {
        displayName = e.displayName;
        isMonthly = e.isMonthly;
        isDaily = e.isDaily;
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
