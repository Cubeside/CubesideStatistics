package de.iani.cubesidestats;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import de.iani.cubesidestats.api.StatisticKey;
import de.iani.settings.sql.MySQLConnection;
import de.iani.settings.sql.SQLConnection;
import de.iani.settings.sql.SQLRunnable;

public class StatisticsDatabase {
    private SQLConnection connection;
    // private final CubesideStatistics plugin;

    private final String getConfigValue;
    private final String increaseConfigValue;

    private final String getPlayerId;
    private final String createPlayerId;

    private final String getAllStatsKeys;
    private final String createStatsKey;
    private final String updateStatsKey;

    private final String changeScore;
    private final String setScore;
    private final String getScore;

    private final String configSettingSerial = "serial";

    public StatisticsDatabase(CubesideStatistics plugin, SQLConfig config) throws SQLException {
        // this.plugin = plugin;
        connection = new MySQLConnection(config.getHost(), config.getDatabase(), config.getUser(), config.getPassword());
        String prefix = config.getTablePrefix();
        updateTables(prefix);

        getConfigValue = "SELECT value FROM " + prefix + "_config WHERE setting = ?";
        increaseConfigValue = "UPDATE " + prefix + "_config set value = value + 1 WHERE setting = ?";
        getPlayerId = "SELECT id FROM " + prefix + "_players WHERE uuid = ?";
        createPlayerId = "INSERT INTO " + prefix + "_players (uuid) VALUE (?)";

        getAllStatsKeys = "SELECT id, name, properties FROM " + prefix + "_stats";
        createStatsKey = "INSERT IGNORE INTO " + prefix + "_stats (name, properties) VALUE (?, ?)";
        updateStatsKey = "UPDATE " + prefix + "_stats SET properties = ? WHERE id = ?";

        changeScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score) VALUE (?, ?, ?, ?) ON DUPLICATE KEY UPDATE score = score + ?";
        setScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score) VALUE (?, ?, ?, ?) ON DUPLICATE KEY UPDATE score = ?";
        getScore = "SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?";
    }

    private void updateTables(String prefix) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                Statement smt = connection.createStatement();
                if (!sqlConnection.hasTable(prefix + "_config")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_config` (" + //
                    " `setting` varchar(50)" + //
                    " `value` int(11)" + //
                    " PRIMARY KEY (`setting`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_players")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_players` (" + //
                    " `id` int(11) AUTO_INCREMENT" + //
                    " `uuid` char(36) NOT NULL," + //
                    " PRIMARY KEY (`id`), UNIQUE KEY (`uuid`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_stats")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_stats` (" + //
                    " `id` int(11) AUTO_INCREMENT" + //
                    " `name` varchar(`255`) NOT NULL," + //
                    " `properties` text NOT NULL," + //
                    " PRIMARY KEY (`id`), UNIQUE KEY (`name`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_scores")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_scores` (" + //
                    " `playerid` int(11) NOT NULL" + //
                    " `statsid` int(11) NOT NULL" + //
                    " `month` int(11) NOT NULL," + //
                    " `score` int(11) NOT NULL," + //
                    " PRIMARY KEY (`playerid`,`month`,`statsid`), KEY (`statsid`,`month`,`score`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                return null;
            }
        });
    }

    public void disconnect() {
        connection.disconnect();
    }

    public class ConfigDTO {
        private final int configSerial;
        private final Collection<StatisticKeyImplementation> statisticKeys;

        public ConfigDTO(int configSerial, Collection<StatisticKeyImplementation> statisticKeys) {
            this.configSerial = configSerial;
            this.statisticKeys = statisticKeys;
        }

        public int getConfigSerial() {
            return configSerial;
        }

        public Collection<StatisticKeyImplementation> getStatisticKeys() {
            return statisticKeys;
        }
    }

    public StatisticKey createStatisticKey(String name) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<StatisticKey>() {
            @Override
            public StatisticKey execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(createStatsKey, Statement.RETURN_GENERATED_KEYS);
                smt.setString(1, name);
                smt.setString(2, "");
                smt.executeUpdate();

                Integer id = null;
                ResultSet results = smt.getGeneratedKeys();
                if (results.next()) {
                    id = results.getInt(1);
                }
                results.close();

                if (id == null) {
                    return null;
                }
                internalIncreaseConfigSerial(connection, sqlConnection);
                return new StatisticKeyImplementation(id, name, "");
            }
        });
    }

    public void updateStatisticKey(StatisticKey key) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                StatisticKeyImplementation impl = (StatisticKeyImplementation) key;
                PreparedStatement smt = sqlConnection.getOrCreateStatement(updateStatsKey);
                smt.setInt(1, impl.getId());
                smt.setString(2, impl.getSerializedProperties());
                smt.executeUpdate();
                internalIncreaseConfigSerial(connection, sqlConnection);
                return null;
            }
        });
    }

    protected void internalIncreaseConfigSerial(Connection connection, SQLConnection sqlConnection) throws SQLException {
        PreparedStatement smt = sqlConnection.getOrCreateStatement(increaseConfigValue);
        smt.setString(1, configSettingSerial);
        smt.executeUpdate();
    }

    public ConfigDTO loadConfig(int oldSerial) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<ConfigDTO>() {
            @Override
            public ConfigDTO execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getConfigValue);
                smt.setString(1, configSettingSerial);
                ResultSet results = smt.executeQuery();
                int configSerial = 0;
                if (results.next()) {
                    configSerial = results.getInt("value");
                }
                results.close();
                if (configSerial <= oldSerial) {
                    return null;
                }

                ArrayList<StatisticKeyImplementation> keys = new ArrayList<>();

                smt = sqlConnection.getOrCreateStatement(getAllStatsKeys);
                results = smt.executeQuery();
                while (results.next()) {
                    keys.add(new StatisticKeyImplementation(results.getInt("id"), results.getString("name"), results.getString("properties")));
                    results.close();
                }

                return new ConfigDTO(configSerial, keys);
            }
        });
    }

    public int getOrCreatePlayerId(final UUID player) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getPlayerId);
                smt.setString(1, player.toString());
                ResultSet results = smt.executeQuery();
                Integer rv = null;
                if (results.next()) {
                    rv = results.getInt("id");
                }
                results.close();
                if (rv == null) {
                    smt = sqlConnection.getOrCreateStatement(createPlayerId, Statement.RETURN_GENERATED_KEYS);
                    smt.setString(1, player.toString());
                    smt.executeUpdate();
                    results = smt.getGeneratedKeys();
                    if (results.next()) {
                        rv = results.getInt(1);
                    }
                    results.close();
                }
                if (rv == null) {
                    throw new SQLException("Could not generate player id");
                }
                return rv;
            }
        });
    }

    public void increaseScore(int databaseId, StatisticKeyImplementation key, int month, int amount) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(changeScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, -1);
                smt.setInt(4, amount);
                smt.setInt(5, amount);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(3, month);
                    smt.executeUpdate();
                }
                return null;
            }
        });
    }

    public void setScore(int databaseId, StatisticKeyImplementation key, int month, int value) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(setScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, -1);
                smt.setInt(4, value);
                smt.setInt(5, value);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(3, month);
                    smt.executeUpdate();
                }
                return null;
            }
        });
    }

    public Integer getScore(int databaseId, StatisticKeyImplementation key, int month) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, month);
                ResultSet results = smt.executeQuery();
                Integer rv = null;
                if (results.next()) {
                    rv = results.getInt("id");
                }
                results.close();
                return rv == null ? 0 : rv;
            }
        });
    }
}
