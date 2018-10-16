package de.iani.cubesidestats;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import de.iani.cubesidestats.sql.MySQLConnection;
import de.iani.cubesidestats.sql.SQLConnection;
import de.iani.cubesidestats.sql.SQLRunnable;

public class StatisticsDatabase {
    private SQLConnection connection;
    private final CubesideStatisticsImplementation impl;

    private final String getConfigValue;
    private final String increaseConfigValue;

    private final String getPlayerId;
    private final String createPlayerId;

    private final String getAllStatsKeys;
    private final String createStatsKey;
    private final String updateStatsKey;

    private final String getAllAchivementKeys;
    private final String createAchivementKey;
    private final String updateAchivementKey;

    private final String setAchivementLevel;
    private final String getAchivementLevel;
    private final String maxAchivementLevel;

    private final String changeScore;
    private final String setScore;
    private final String maxScore;
    private final String minScore;
    private final String getScore;
    // private final String getPosition;
    private final String getTopScores;

    private final String deleteThisServersPlayers;
    private final String updateThisServerPlayers;
    private final String getAllServersPlayers;

    private final String configSettingSerial = "serial";

    public StatisticsDatabase(CubesideStatisticsImplementation impl, SQLConfig config) throws SQLException {
        this.impl = impl;
        connection = new MySQLConnection(config.getHost(), config.getDatabase(), config.getUser(), config.getPassword());
        String prefix = config.getTablePrefix();
        updateTables(prefix);

        getConfigValue = "SELECT value FROM " + prefix + "_config WHERE setting = ?";
        increaseConfigValue = "INSERT INTO " + prefix + "_config (setting, `value`) VALUE (?, 1) ON DUPLICATE KEY UPDATE `value` = `value` + 1";
        getPlayerId = "SELECT id FROM " + prefix + "_players WHERE uuid = ?";
        createPlayerId = "INSERT INTO " + prefix + "_players (uuid) VALUE (?)";

        getAllStatsKeys = "SELECT id, name, properties FROM " + prefix + "_stats";
        createStatsKey = "INSERT IGNORE INTO " + prefix + "_stats (name, properties) VALUE (?, ?)";
        updateStatsKey = "UPDATE " + prefix + "_stats SET properties = ? WHERE id = ?";

        getAllAchivementKeys = "SELECT id, name, properties FROM " + prefix + "_achivementkeys";
        createAchivementKey = "INSERT IGNORE INTO " + prefix + "_achivementkeys (name, properties) VALUE (?, ?)";
        updateAchivementKey = "UPDATE " + prefix + "_achivementkeys SET properties = ? WHERE id = ?";

        setAchivementLevel = "INSERT INTO " + prefix + "_achivements (playerid, achivmenentid, level) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE level = ?";
        maxAchivementLevel = "INSERT INTO " + prefix + "_achivements (playerid, achivmenentid, level) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE level = GREATEST(level,?)";
        getAchivementLevel = "SELECT level FROM " + prefix + "_achivements WHERE playerid = ? AND achivmenentid = ?";

        changeScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score) VALUE (?, ?, ?, ?) ON DUPLICATE KEY UPDATE score = score + ?";
        setScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score) VALUE (?, ?, ?, ?) ON DUPLICATE KEY UPDATE score = ?";
        maxScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score) VALUE (?, ?, ?, ?) ON DUPLICATE KEY UPDATE score = GREATEST(score,?)";
        minScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score) VALUE (?, ?, ?, ?) ON DUPLICATE KEY UPDATE score = LEAST(score,?)";
        getScore = "SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?";
        // getPosition = "SELECT COUNT(*) as count FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score < (SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?)";

        getTopScores = "SELECT uuid, score FROM " + prefix + "_scores sc LEFT JOIN " + prefix + "_players st ON (sc.playerid = st.id) WHERE statsid = ? AND month = ? ORDER BY score DESC LIMIT ?";

        deleteThisServersPlayers = "DELETE FROM " + prefix + "_current_players WHERE server = ?";
        updateThisServerPlayers = "INSERT INTO " + prefix + "_current_players (server, game, players) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE players = ?";
        getAllServersPlayers = "SELECT game, SUM(players) as playersum FROM " + prefix + "_current_players  WHERE server != ? GROUP BY game";
    }

    private void updateTables(String prefix) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                Statement smt = connection.createStatement();
                if (!sqlConnection.hasTable(prefix + "_config")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_config` (" + //
                    " `setting` varchar(50)," + //
                    " `value` int(11)," + //
                    " PRIMARY KEY (`setting`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_players")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_players` (" + //
                    " `id` int(11) AUTO_INCREMENT," + //
                    " `uuid` char(36) NOT NULL," + //
                    " PRIMARY KEY (`id`), UNIQUE KEY (`uuid`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_stats")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_stats` (" + //
                    " `id` int(11) AUTO_INCREMENT," + //
                    " `name` varchar(255) NOT NULL," + //
                    " `properties` text NOT NULL," + //
                    " PRIMARY KEY (`id`), UNIQUE KEY (`name`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_scores")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_scores` (" + //
                    " `playerid` int(11) NOT NULL," + //
                    " `statsid` int(11) NOT NULL," + //
                    " `month` int(11) NOT NULL," + //
                    " `score` int(11) NOT NULL," + //
                    " PRIMARY KEY (`playerid`,`month`,`statsid`), KEY (`statsid`,`month`,`score`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_current_players")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_current_players` (" + //
                    " `server` char(36) NOT NULL," + //
                    " `game` varchar(100) NOT NULL," + //
                    " `players` int(11) NOT NULL," + //
                    " PRIMARY KEY (`game`,`server`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_achivementkeys")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_achivementkeys` (" + //
                    " `id` int(11) AUTO_INCREMENT," + //
                    " `name` varchar(255) NOT NULL," + //
                    " `properties` text NOT NULL," + //
                    " PRIMARY KEY (`id`), UNIQUE KEY (`name`)" + //
                    " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_achivements")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_achivements` (" + //
                    " `playerid` int(11) NOT NULL," + //
                    " `achivmenentid` int(11) NOT NULL," + //
                    " `level` int(11) NOT NULL," + //
                    " PRIMARY KEY (`playerid`,`achivmenentid`)" + //
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
        private final Collection<AchivementKeyImplementation> achivementKeys;

        public ConfigDTO(int configSerial, Collection<StatisticKeyImplementation> statisticKeys, Collection<AchivementKeyImplementation> achivementKeys) {
            this.configSerial = configSerial;
            this.statisticKeys = statisticKeys;
            this.achivementKeys = achivementKeys;
        }

        public int getConfigSerial() {
            return configSerial;
        }

        public Collection<StatisticKeyImplementation> getStatisticKeys() {
            return statisticKeys;
        }

        public Collection<AchivementKeyImplementation> getAchivementKeys() {
            return achivementKeys;
        }
    }

    public StatisticKeyImplementation createStatisticKey(String name) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<StatisticKeyImplementation>() {
            @Override
            public StatisticKeyImplementation execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
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
                return new StatisticKeyImplementation(id, name, null, impl);
            }
        });
    }

    public void updateStatisticKey(StatisticKeyImplementation impl) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(updateStatsKey);
                smt.setString(1, impl.getSerializedProperties());
                smt.setInt(2, impl.getId());
                // int rows =
                smt.executeUpdate();
                // StatisticsDatabase.this.impl.getPlugin().getLogger().info(impl.getId() + " - " + impl.getSerializedProperties());
                // StatisticsDatabase.this.impl.getPlugin().getLogger().info("Rows: " + rows);
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
                    keys.add(new StatisticKeyImplementation(results.getInt("id"), results.getString("name"), results.getString("properties"), impl));
                }
                results.close();

                ArrayList<AchivementKeyImplementation> achivkeys = new ArrayList<>();

                smt = sqlConnection.getOrCreateStatement(getAllAchivementKeys);
                results = smt.executeQuery();
                while (results.next()) {
                    achivkeys.add(new AchivementKeyImplementation(results.getInt("id"), results.getString("name"), results.getString("properties"), impl));
                }
                results.close();

                return new ConfigDTO(configSerial, keys, achivkeys);
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

    public void increaseScore(int databaseId, StatisticKeyImplementation key, int month, int day, int amount) throws SQLException {
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
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(3, day);
                    smt.executeUpdate();
                }
                return null;
            }
        });
    }

    public void setScore(int databaseId, StatisticKeyImplementation key, int month, int day, int value) throws SQLException {
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
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(3, day);
                    smt.executeUpdate();
                }
                return null;
            }
        });
    }

    public boolean maxScore(int databaseId, StatisticKeyImplementation key, int month, int day, int value) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Boolean>() {
            @Override
            public Boolean execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, -1);
                ResultSet results = smt.executeQuery();
                Integer old = null;
                if (results.next()) {
                    old = results.getInt("score");
                }
                results.close();

                smt = sqlConnection.getOrCreateStatement(maxScore);
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
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(3, day);
                    smt.executeUpdate();
                }
                return old == null || value > old.intValue();
            }
        });
    }

    public boolean minScore(int databaseId, StatisticKeyImplementation key, int month, int day, int value) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Boolean>() {
            @Override
            public Boolean execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, -1);
                ResultSet results = smt.executeQuery();
                Integer old = null;
                if (results.next()) {
                    old = results.getInt("score");
                }
                results.close();

                smt = sqlConnection.getOrCreateStatement(minScore);
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
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(3, day);
                    smt.executeUpdate();
                }
                return old == null || value < old.intValue();
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
                    rv = results.getInt("score");
                }
                results.close();
                return rv == null ? 0 : rv;
            }
        });
    }

    public List<InternalPlayerWithScore> getTop(StatisticKeyImplementation key, int count, int month) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<List<InternalPlayerWithScore>>() {
            @Override
            public List<InternalPlayerWithScore> execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getTopScores);
                smt.setInt(1, keyId);
                smt.setInt(2, month);
                smt.setInt(3, count);
                ResultSet results = smt.executeQuery();
                ArrayList<InternalPlayerWithScore> rv = new ArrayList<>();
                int position = 0;
                while (results.next()) {
                    UUID player = UUID.fromString(results.getString("uuid"));
                    position += 1;
                    int score = results.getInt("score");
                    rv.add(new InternalPlayerWithScore(player, score, position));
                }
                results.close();
                return rv;
            }
        });
    }

    public void deleteGamePlayers(UUID serverid) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(deleteThisServersPlayers);
                smt.setString(1, serverid.toString());
                smt.executeUpdate();
                return null;
            }
        });
    }

    public void setGamePlayers(UUID serverid, String game, int players) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(updateThisServerPlayers);
                smt.setString(1, serverid.toString());
                smt.setString(2, game);
                smt.setInt(3, players);
                smt.setInt(4, players);
                smt.executeUpdate();
                return null;
            }
        });
    }

    public HashMap<String, Integer> getGamePlayers(UUID ignoreserverid) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getAllServersPlayers);
                smt.setString(1, ignoreserverid.toString());
                ResultSet rs = smt.executeQuery();
                HashMap<String, Integer> rv = new HashMap<>();
                while (rs.next()) {
                    String game = rs.getString("game");
                    int players = rs.getInt("playersum");
                    rv.put(game, players);
                }
                return rv;
            }
        });
    }

    public AchivementKeyImplementation createAchivementKey(String name) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<AchivementKeyImplementation>() {
            @Override
            public AchivementKeyImplementation execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(createAchivementKey, Statement.RETURN_GENERATED_KEYS);
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
                return new AchivementKeyImplementation(id, name, null, impl);
            }
        });
    }

    public void updateAchivementKey(AchivementKeyImplementation impl) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(updateAchivementKey);
                smt.setString(1, impl.getSerializedProperties());
                smt.setInt(2, impl.getId());
                // int rows =
                smt.executeUpdate();
                // StatisticsDatabase.this.impl.getPlugin().getLogger().info(impl.getId() + " - " + impl.getSerializedProperties());
                // StatisticsDatabase.this.impl.getPlugin().getLogger().info("Rows: " + rows);
                internalIncreaseConfigSerial(connection, sqlConnection);
                return null;
            }
        });
    }

    public Integer getAchivementLevel(int databaseId, AchivementKeyImplementation key) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                return internalGetLevel(databaseId, sqlConnection, keyId);
            }
        });
    }

    public Integer setAchivementLevel(int databaseId, AchivementKeyImplementation key, int level, boolean queryOld) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                Integer oldLevel = queryOld ? internalGetLevel(databaseId, sqlConnection, keyId) : null;
                PreparedStatement smt = sqlConnection.getOrCreateStatement(setAchivementLevel);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, level);
                smt.setInt(4, level);
                smt.executeUpdate();
                return oldLevel;
            }
        });
    }

    public Integer maxAchivementLevel(int databaseId, AchivementKeyImplementation key, int level, boolean queryOld) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                Integer oldLevel = queryOld ? internalGetLevel(databaseId, sqlConnection, keyId) : null;
                PreparedStatement smt = sqlConnection.getOrCreateStatement(maxAchivementLevel);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, level);
                smt.setInt(4, level);
                smt.executeUpdate();
                return oldLevel;
            }
        });
    }

    protected Integer internalGetLevel(int databaseId, SQLConnection sqlConnection, int keyId) throws SQLException {
        PreparedStatement smt = sqlConnection.getOrCreateStatement(getAchivementLevel);
        smt.setInt(1, databaseId);
        smt.setInt(2, keyId);
        ResultSet results = smt.executeQuery();
        Integer rv = null;
        if (results.next()) {
            rv = results.getInt("level");
        }
        results.close();
        return rv == null ? 0 : rv;
    }
}
