package de.iani.cubesidestats;

import com.google.common.base.Preconditions;
import de.iani.cubesidestats.api.Ordering;
import de.iani.cubesidestats.api.PositionAlgorithm;
import de.iani.cubesideutils.sql.MySQLConnection;
import de.iani.cubesideutils.sql.SQLConnection;
import de.iani.cubesideutils.sql.SQLRunnable;
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

public class StatisticsDatabase {
    private SQLConnection connection;
    private final CubesideStatisticsImplementation impl;

    private final String getConfigValue;
    private final String increaseConfigValue;

    private final String getPlayerId;
    private final String createPlayerId;

    private final String getAllGlobalStatsKeys;
    private final String createGlobalStatsKey;
    private final String updateGlobalStatsKey;

    private final String changeGlobalStatsValue;
    private final String setGlobalStatsValue;
    private final String maxGlobalStatsValue;
    private final String minGlobalStatsValue;
    private final String getGlobalStatsValue;

    private final String getAllStatsKeys;
    private final String createStatsKey;
    private final String updateStatsKey;

    private final String changeScore;
    private final String setScore;
    private final String maxScore;
    private final String minScore;
    private final String getScore;
    private final String deleteScore;
    private final String getThreeScores;
    private final String getPositionDescending;
    private final String getPositionAscending;
    private final String getPositionMaxTotalOrder;
    private final String getPositionMinTotalOrder;

    private final String getTopScoresDesc;
    private final String getTopScoresAsc;
    private final String getScoreEntries;
    private final String getPositionDescendingFromScore;
    private final String getPositionAscendingFromScore;
    private final String getPositionDescendingFromDistinctScore;
    private final String getPositionAscendingFromDistinctScore;

    private final String getAllAchivementKeys;
    private final String createAchivementKey;
    private final String updateAchivementKey;

    private final String setAchivementLevel;
    private final String getAchivementLevel;
    private final String maxAchivementLevel;

    private final String getAllSettingKeys;
    private final String createSettingKey;
    private final String updateSettingKey;

    private final String setSettingValue;
    private final String getSettingValue;
    private final String getSettingValuesPlayer;

    private final String deleteThisServersPlayers;
    private final String updateThisServerPlayers;
    private final String getAllServersPlayers;

    private final String configSettingSerial = "serial";

    public StatisticsDatabase(CubesideStatisticsImplementation impl, SQLConfig config) throws SQLException {
        this.impl = impl;
        connection = new MySQLConnection(config.getHost(), config.getDatabase(), config.getUser(), config.getPassword());
        String prefix = config.getTablePrefix();
        if (config.isCheckTables()) {
            updateTables(prefix);
        }

        getConfigValue = "SELECT value FROM " + prefix + "_config WHERE setting = ?";
        increaseConfigValue = "INSERT INTO " + prefix + "_config (setting, `value`) VALUE (?, 1) ON DUPLICATE KEY UPDATE `value` = `value` + 1";
        getPlayerId = "SELECT id FROM " + prefix + "_players WHERE uuid = ?";
        createPlayerId = "INSERT INTO " + prefix + "_players (uuid) VALUE (?)";

        getAllGlobalStatsKeys = "SELECT id, name, properties FROM " + prefix + "_globalstats";
        createGlobalStatsKey = "INSERT IGNORE INTO " + prefix + "_globalstats (name, properties) VALUE (?, ?)";
        updateGlobalStatsKey = "UPDATE " + prefix + "_globalstats SET properties = ? WHERE id = ?";

        changeGlobalStatsValue = "INSERT INTO " + prefix + "_globalstatsvalues (statsid, month, score) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE score = score + ?";
        setGlobalStatsValue = "INSERT INTO " + prefix + "_globalstatsvalues (statsid, month, score) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE score = ?";
        maxGlobalStatsValue = "INSERT INTO " + prefix + "_globalstatsvalues (statsid, month, score) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE score = GREATEST(score,?)";
        minGlobalStatsValue = "INSERT INTO " + prefix + "_globalstatsvalues (statsid, month, score) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE score = LEAST(score,?)";
        getGlobalStatsValue = "SELECT score FROM " + prefix + "_globalstatsvalues WHERE statsid = ? AND month = ?";

        getAllStatsKeys = "SELECT id, name, properties FROM " + prefix + "_stats";
        createStatsKey = "INSERT IGNORE INTO " + prefix + "_stats (name, properties) VALUE (?, ?)";
        updateStatsKey = "UPDATE " + prefix + "_stats SET properties = ? WHERE id = ?";

        changeScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score, updated) VALUE (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE updated = CASE WHEN 0 <> ? THEN ? ELSE updated END, score = score + ?";
        setScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score, updated) VALUE (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE updated = CASE WHEN score <> ? THEN ? ELSE updated END, score = ?";
        maxScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score, updated) VALUE (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE updated = CASE WHEN score < ? THEN ? ELSE updated END, score = GREATEST(score,?)";
        minScore = "INSERT INTO " + prefix + "_scores (playerid, statsid, month, score, updated) VALUE (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE updated = CASE WHEN score > ? THEN ? ELSE updated END, score = LEAST(score,?)";
        deleteScore = "DELETE FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ?";

        getScore = "SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?";
        getThreeScores = "SELECT score, month FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month IN (?,?,?)";
        getPositionDescending = "SELECT COUNT(*) as count FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score > (SELECT MAX(score) as score FROM (SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ? UNION SELECT 0 as score) as t)";
        getPositionAscending = "SELECT COUNT(*) as count FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score < (SELECT MIN(score) as score FROM (SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ? UNION SELECT 2147483647 as score) as t)";

        getPositionMaxTotalOrder = "SELECT SUM(t2.ct) as count FROM (SELECT COUNT(*) as ct FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score = IFNULL("
                + "(SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?), -2147483647) "
                + "AND (updated < IFNULL((SELECT updated FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?), 9223372036854775808) "
                + "OR (updated = IFNULL((SELECT updated FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?), 9223372036854775808) AND playerid > ?)) "
                + "UNION SELECT COUNT(*) as ct FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score > IFNULL("
                + "(SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?), 0)) as t2";

        getPositionMinTotalOrder = "SELECT SUM(t2.ct) as count FROM (SELECT COUNT(*) as ct FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score = IFNULL("
                + "(SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?), 2147483647) "
                + "AND (updated < IFNULL((SELECT updated FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?), 9223372036854775808) "
                + "OR (updated = IFNULL((SELECT updated FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?), 9223372036854775808) AND playerid > ?)) "
                + "UNION SELECT COUNT(*) as ct FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score < IFNULL("
                + "(SELECT score FROM " + prefix + "_scores WHERE playerid = ? AND statsid = ? AND month = ?), 2147483647)) as t2";

        getTopScoresDesc = "SELECT uuid, score FROM " + prefix + "_scores sc LEFT JOIN " + prefix + "_players st ON (sc.playerid = st.id) WHERE statsid = ? AND month = ? ORDER BY score DESC, updated ASC, playerid DESC LIMIT ?, ?";
        getTopScoresAsc = "SELECT uuid, score FROM " + prefix + "_scores sc LEFT JOIN " + prefix + "_players st ON (sc.playerid = st.id) WHERE statsid = ? AND month = ? ORDER BY score ASC, updated ASC, playerid DESC LIMIT ?, ?";
        getScoreEntries = "SELECT COUNT(*) as counter FROM " + prefix + "_scores WHERE statsid = ? AND month = ?";
        getPositionDescendingFromScore = "SELECT COUNT(*) as count FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score > ?";
        getPositionAscendingFromScore = "SELECT COUNT(*) as count FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score < ?";
        getPositionDescendingFromDistinctScore = "SELECT COUNT(DISTINCT score) as count FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score > ?";
        getPositionAscendingFromDistinctScore = "SELECT COUNT(DISTINCT score) as count FROM " + prefix + "_scores WHERE statsid = ? AND month = ? AND score < ?";

        getAllAchivementKeys = "SELECT id, name, properties FROM " + prefix + "_achivementkeys";
        createAchivementKey = "INSERT IGNORE INTO " + prefix + "_achivementkeys (name, properties) VALUE (?, ?)";
        updateAchivementKey = "UPDATE " + prefix + "_achivementkeys SET properties = ? WHERE id = ?";

        setAchivementLevel = "INSERT INTO " + prefix + "_achivements (playerid, achivmenentid, level) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE level = ?";
        maxAchivementLevel = "INSERT INTO " + prefix + "_achivements (playerid, achivmenentid, level) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE level = GREATEST(level,?)";
        getAchivementLevel = "SELECT level FROM " + prefix + "_achivements WHERE playerid = ? AND achivmenentid = ?";

        getAllSettingKeys = "SELECT id, name, properties FROM " + prefix + "_settingkeys";
        createSettingKey = "INSERT IGNORE INTO " + prefix + "_settingkeys (name, properties) VALUE (?, ?)";
        updateSettingKey = "UPDATE " + prefix + "_settingkeys SET properties = ? WHERE id = ?";

        setSettingValue = "INSERT INTO " + prefix + "_settings (playerid, settingid, value) VALUE (?, ?, ?) ON DUPLICATE KEY UPDATE value = ?";
        getSettingValue = "SELECT value FROM " + prefix + "_settings WHERE playerid = ? AND settingid = ?";
        getSettingValuesPlayer = "SELECT settingid, value FROM " + prefix + "_settings WHERE playerid = ?";

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
                            " `setting` varchar(50) CHARACTER SET utf8 COLLATE utf8_bin," + //
                            " `value` int(11)," + //
                            " PRIMARY KEY (`setting`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                } else {
                    ResultSet rs = smt.executeQuery("SHOW FULL COLUMNS FROM `" + prefix + "_config` WHERE Field = \"setting\" AND Collation = \"utf8_general_ci\"");
                    if (rs.next()) {
                        smt.executeUpdate("ALTER TABLE `" + prefix + "_config` CHANGE `setting` `setting` VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL");
                    }
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
                            " `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL," + //
                            " `properties` text NOT NULL," + //
                            " PRIMARY KEY (`id`), UNIQUE KEY (`name`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                } else {
                    ResultSet rs = smt.executeQuery("SHOW FULL COLUMNS FROM `" + prefix + "_stats` WHERE Field = \"name\" AND Collation = \"utf8_general_ci\"");
                    if (rs.next()) {
                        smt.executeUpdate("ALTER TABLE `" + prefix + "_stats` CHANGE `name` `name` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL");
                    }
                }
                if (!sqlConnection.hasTable(prefix + "_scores")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_scores` (" + //
                            " `playerid` int(11) NOT NULL," + //
                            " `statsid` int(11) NOT NULL," + //
                            " `month` int(11) NOT NULL," + //
                            " `score` int(11) NOT NULL," + //
                            " `updated` BIGINT(20) NOT NULL DEFAULT 0," + //
                            " PRIMARY KEY (`playerid`,`month`,`statsid`), KEY (`statsid`,`month`,`score`,`updated`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                } else {
                    if (!sqlConnection.hasColumn(prefix + "_scores", "updated")) {
                        smt.executeUpdate("ALTER TABLE `" + prefix + "_scores` ADD `updated` BIGINT(20) NOT NULL DEFAULT '0' AFTER `score`");
                        smt.executeUpdate("ALTER TABLE `" + prefix + "_scores` DROP INDEX `statsid`, ADD INDEX `statsid` (`statsid`, `month`, `score`, `updated`, `playerid`");
                    }
                }
                if (!sqlConnection.hasTable(prefix + "_globalstats")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_globalstats` (" + //
                            " `id` int(11) AUTO_INCREMENT," + //
                            " `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL," + //
                            " `properties` text NOT NULL," + //
                            " PRIMARY KEY (`id`), UNIQUE KEY (`name`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                } else {
                    ResultSet rs = smt.executeQuery("SHOW FULL COLUMNS FROM `" + prefix + "_globalstats` WHERE Field = \"name\" AND Collation = \"utf8_general_ci\"");
                    if (rs.next()) {
                        smt.executeUpdate("ALTER TABLE `" + prefix + "_globalstats` CHANGE `name` `name` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL");
                    }
                }
                if (!sqlConnection.hasTable(prefix + "_globalstatsvalues")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_globalstatsvalues` (" + //
                            " `statsid` int(11) NOT NULL," + //
                            " `month` int(11) NOT NULL," + //
                            " `score` int(11) NOT NULL," + //
                            " PRIMARY KEY (`month`,`statsid`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }
                if (!sqlConnection.hasTable(prefix + "_current_players")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_current_players` (" + //
                            " `server` char(36) NOT NULL," + //
                            " `game` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL," + //
                            " `players` int(11) NOT NULL," + //
                            " PRIMARY KEY (`game`,`server`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                } else {
                    ResultSet rs = smt.executeQuery("SHOW FULL COLUMNS FROM `" + prefix + "_current_players` WHERE Field = \"game\" AND Collation = \"utf8_general_ci\"");
                    if (rs.next()) {
                        smt.executeUpdate("ALTER TABLE `" + prefix + "_current_players` CHANGE `game` `game` VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL");
                    }
                    rs.close();
                }
                if (!sqlConnection.hasTable(prefix + "_achivementkeys")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_achivementkeys` (" + //
                            " `id` int(11) AUTO_INCREMENT," + //
                            " `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL," + //
                            " `properties` text NOT NULL," + //
                            " PRIMARY KEY (`id`), UNIQUE KEY (`name`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                } else {
                    ResultSet rs = smt.executeQuery("SHOW FULL COLUMNS FROM `" + prefix + "_achivementkeys` WHERE Field = \"name\" AND Collation = \"utf8_general_ci\"");
                    if (rs.next()) {
                        smt.executeUpdate("ALTER TABLE `" + prefix + "_achivementkeys` CHANGE `name` `name` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL");
                    }
                }
                if (!sqlConnection.hasTable(prefix + "_achivements")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_achivements` (" + //
                            " `playerid` int(11) NOT NULL," + //
                            " `achivmenentid` int(11) NOT NULL," + //
                            " `level` int(11) NOT NULL," + //
                            " PRIMARY KEY (`playerid`,`achivmenentid`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                }

                if (!sqlConnection.hasTable(prefix + "_settingkeys")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_settingkeys` (" + //
                            " `id` int(11) AUTO_INCREMENT," + //
                            " `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL," + //
                            " `properties` text NOT NULL," + //
                            " PRIMARY KEY (`id`), UNIQUE KEY (`name`)" + //
                            " ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
                } else {
                    ResultSet rs = smt.executeQuery("SHOW FULL COLUMNS FROM `" + prefix + "_settingkeys` WHERE Field = \"name\" AND Collation = \"utf8_general_ci\"");
                    if (rs.next()) {
                        smt.executeUpdate("ALTER TABLE `" + prefix + "_settingkeys` CHANGE `name` `name` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL");
                    }
                }
                if (!sqlConnection.hasTable(prefix + "_settings")) {
                    smt.executeUpdate("CREATE TABLE IF NOT EXISTS `" + prefix + "_settings` (" + //
                            " `playerid` int(11) NOT NULL," + //
                            " `settingid` int(11) NOT NULL," + //
                            " `value` int(11) NOT NULL," + //
                            " PRIMARY KEY (`playerid`,`settingid`)" + //
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
        private final Collection<GlobalStatisticKeyImplementation> globalStatisticKeys;
        private final Collection<StatisticKeyImplementation> statisticKeys;
        private final Collection<AchivementKeyImplementation> achivementKeys;
        private final Collection<SettingKeyImplementation> settingKeys;

        public ConfigDTO(int configSerial, Collection<GlobalStatisticKeyImplementation> globalStatisticKeys, Collection<StatisticKeyImplementation> statisticKeys, Collection<AchivementKeyImplementation> achivementKeys, Collection<SettingKeyImplementation> settingKeys) {
            this.globalStatisticKeys = globalStatisticKeys;
            this.configSerial = configSerial;
            this.statisticKeys = statisticKeys;
            this.achivementKeys = achivementKeys;
            this.settingKeys = settingKeys;
        }

        public int getConfigSerial() {
            return configSerial;
        }

        public Collection<GlobalStatisticKeyImplementation> getGlobalStatisticKeys() {
            return globalStatisticKeys;
        }

        public Collection<StatisticKeyImplementation> getStatisticKeys() {
            return statisticKeys;
        }

        public Collection<AchivementKeyImplementation> getAchivementKeys() {
            return achivementKeys;
        }

        public Collection<SettingKeyImplementation> getSettingKeys() {
            return settingKeys;
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

    public GlobalStatisticKeyImplementation createGlobalStatisticKey(String name) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<GlobalStatisticKeyImplementation>() {
            @Override
            public GlobalStatisticKeyImplementation execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(createGlobalStatsKey, Statement.RETURN_GENERATED_KEYS);
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
                return new GlobalStatisticKeyImplementation(id, name, null, impl);
            }
        });
    }

    public void updateGlobalStatisticKey(GlobalStatisticKeyImplementation impl) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(updateGlobalStatsKey);
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

                ArrayList<GlobalStatisticKeyImplementation> globalStatskeys = new ArrayList<>();
                smt = sqlConnection.getOrCreateStatement(getAllGlobalStatsKeys);
                results = smt.executeQuery();
                while (results.next()) {
                    globalStatskeys.add(new GlobalStatisticKeyImplementation(results.getInt("id"), results.getString("name"), results.getString("properties"), impl));
                }
                results.close();

                ArrayList<StatisticKeyImplementation> statskeys = new ArrayList<>();
                smt = sqlConnection.getOrCreateStatement(getAllStatsKeys);
                results = smt.executeQuery();
                while (results.next()) {
                    statskeys.add(new StatisticKeyImplementation(results.getInt("id"), results.getString("name"), results.getString("properties"), impl));
                }
                results.close();

                ArrayList<AchivementKeyImplementation> achivkeys = new ArrayList<>();
                smt = sqlConnection.getOrCreateStatement(getAllAchivementKeys);
                results = smt.executeQuery();
                while (results.next()) {
                    achivkeys.add(new AchivementKeyImplementation(results.getInt("id"), results.getString("name"), results.getString("properties"), impl));
                }
                results.close();

                ArrayList<SettingKeyImplementation> settingkeys = new ArrayList<>();
                smt = sqlConnection.getOrCreateStatement(getAllSettingKeys);
                results = smt.executeQuery();
                while (results.next()) {
                    settingkeys.add(new SettingKeyImplementation(results.getInt("id"), results.getString("name"), results.getString("properties"), impl));
                }
                results.close();

                return new ConfigDTO(configSerial, globalStatskeys, statskeys, achivkeys, settingkeys);
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

    public void increaseGlobalStatsValue(GlobalStatisticKeyImplementation key, int month, int day, int amount) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(changeGlobalStatsValue);
                smt.setInt(1, keyId);
                smt.setInt(2, -1);
                smt.setInt(3, amount);
                smt.setInt(4, amount);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(2, month);
                    smt.executeUpdate();
                }
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(2, day);
                    smt.executeUpdate();
                }
                return null;
            }
        });
    }

    public void setGlobalStatsValue(GlobalStatisticKeyImplementation key, int month, int day, int value) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(setGlobalStatsValue);
                smt.setInt(1, keyId);
                smt.setInt(2, -1);
                smt.setInt(3, value);
                smt.setInt(4, value);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(2, month);
                    smt.executeUpdate();
                }
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(2, day);
                    smt.executeUpdate();
                }
                return null;
            }
        });
    }

    public boolean maxGlobalStatsValue(GlobalStatisticKeyImplementation key, int month, int day, int value) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Boolean>() {
            @Override
            public Boolean execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getGlobalStatsValue);
                smt.setInt(1, keyId);
                smt.setInt(2, -1);
                ResultSet results = smt.executeQuery();
                Integer old = null;
                if (results.next()) {
                    old = results.getInt("score");
                }
                results.close();

                smt = sqlConnection.getOrCreateStatement(maxGlobalStatsValue);
                smt.setInt(1, keyId);
                smt.setInt(2, -1);
                smt.setInt(3, value);
                smt.setInt(4, value);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(2, month);
                    smt.executeUpdate();
                }
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(2, day);
                    smt.executeUpdate();
                }
                return old == null || value > old.intValue();
            }
        });
    }

    public boolean minGlobalStatsValue(GlobalStatisticKeyImplementation key, int month, int day, int value) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Boolean>() {
            @Override
            public Boolean execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getGlobalStatsValue);
                smt.setInt(1, keyId);
                smt.setInt(2, -1);
                ResultSet results = smt.executeQuery();
                Integer old = null;
                if (results.next()) {
                    old = results.getInt("score");
                }
                results.close();

                smt = sqlConnection.getOrCreateStatement(minGlobalStatsValue);
                smt.setInt(1, keyId);
                smt.setInt(2, -1);
                smt.setInt(3, value);
                smt.setInt(4, value);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(2, month);
                    smt.executeUpdate();
                }
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(2, day);
                    smt.executeUpdate();
                }
                return old == null || value < old.intValue();
            }
        });
    }

    public Integer getGlobalStatsValue(GlobalStatisticKeyImplementation key, int month) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getGlobalStatsValue);
                smt.setInt(1, keyId);
                smt.setInt(2, month);
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

    protected StatsUpdateResultDTO internalGetOldScores(SQLConnection sqlConnection, int databaseId, StatisticKeyImplementation key, int month, int day) throws SQLException {
        return internalGetOldScores(sqlConnection, databaseId, key, month, day, null);
    }

    protected StatsUpdateResultDTO internalGetOldScores(SQLConnection sqlConnection, int databaseId, StatisticKeyImplementation key, int month, int day, Integer defaultValue) throws SQLException {
        int keyId = key.getId();
        PreparedStatement smt = sqlConnection.getOrCreateStatement(getThreeScores);
        smt.setInt(1, databaseId);
        smt.setInt(2, keyId);
        smt.setInt(3, -1);
        smt.setInt(4, key.isMonthlyStats() ? month : -1);
        smt.setInt(5, key.isDailyStats() ? day : -1);
        ResultSet results = smt.executeQuery();
        Integer oldAlltime = defaultValue;
        Integer oldMonth = defaultValue;
        Integer oldDay = defaultValue;
        while (results.next()) {
            int score = results.getInt("score");
            int monthV = results.getInt("month");
            if (monthV == -1) {
                oldAlltime = score;
            } else if (key.isMonthlyStats() && monthV == month) {
                oldMonth = score;
            } else if (key.isDailyStats() && monthV == day) {
                oldDay = score;
            }
        }
        results.close();
        return new StatsUpdateResultDTO(oldAlltime, oldMonth, oldDay);
    }

    public StatsUpdateResultDTO increaseScore(int databaseId, StatisticKeyImplementation key, int month, int day, int amount) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<StatsUpdateResultDTO>() {
            @Override
            public StatsUpdateResultDTO execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                StatsUpdateResultDTO updateResult = internalGetOldScores(sqlConnection, databaseId, key, month, day, 0);
                int keyId = key.getId();
                long time = System.currentTimeMillis();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(changeScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, -1);
                smt.setInt(4, amount);
                smt.setLong(5, time);
                smt.setInt(6, amount);
                smt.setLong(7, time);
                smt.setInt(8, amount);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(3, month);
                    smt.executeUpdate();
                }
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(3, day);
                    smt.executeUpdate();
                }
                updateResult.setNewValues(or(updateResult.getOldAlltime(), 0) + amount, or(updateResult.getOldMonth(), 0) + amount, or(updateResult.getOldDay(), 0) + amount);
                return updateResult;
            }
        });
    }

    protected int or(Integer nullable, int alternative) {
        return nullable != null ? nullable : alternative;
    }

    public StatsUpdateResultDTO setScore(int databaseId, StatisticKeyImplementation key, int month, int day, int value) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<StatsUpdateResultDTO>() {
            @Override
            public StatsUpdateResultDTO execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                StatsUpdateResultDTO updateResult = internalGetOldScores(sqlConnection, databaseId, key, month, day);
                int keyId = key.getId();
                long time = System.currentTimeMillis();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(setScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, -1);
                smt.setInt(4, value);
                smt.setLong(5, time);
                smt.setInt(6, value);
                smt.setLong(7, time);
                smt.setInt(8, value);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(3, month);
                    smt.executeUpdate();
                }
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(3, day);
                    smt.executeUpdate();
                }
                updateResult.setNewValues(value, value, value);
                return updateResult;
            }
        });
    }

    public StatsUpdateResultDTO maxScore(int databaseId, StatisticKeyImplementation key, int month, int day, int value) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<StatsUpdateResultDTO>() {
            @Override
            public StatsUpdateResultDTO execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                StatsUpdateResultDTO updateResult = internalGetOldScores(sqlConnection, databaseId, key, month, day);
                int keyId = key.getId();
                long time = System.currentTimeMillis();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(maxScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, -1);
                smt.setInt(4, value);
                smt.setLong(5, time);
                smt.setInt(6, value);
                smt.setLong(7, time);
                smt.setInt(8, value);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(3, month);
                    smt.executeUpdate();
                }
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(3, day);
                    smt.executeUpdate();
                }
                updateResult.setNewValues(Math.max(or(updateResult.getOldAlltime(), Integer.MIN_VALUE), value), Math.max(or(updateResult.getOldMonth(), Integer.MIN_VALUE), value), Math.max(or(updateResult.getOldDay(), Integer.MIN_VALUE), value));
                return updateResult;
            }
        });
    }

    public StatsUpdateResultDTO minScore(int databaseId, StatisticKeyImplementation key, int month, int day, int value) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<StatsUpdateResultDTO>() {
            @Override
            public StatsUpdateResultDTO execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                StatsUpdateResultDTO updateResult = internalGetOldScores(sqlConnection, databaseId, key, month, day);
                int keyId = key.getId();
                long time = System.currentTimeMillis();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(minScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, -1);
                smt.setInt(4, value);
                smt.setLong(5, time);
                smt.setInt(6, value);
                smt.setLong(7, time);
                smt.setInt(8, value);
                smt.executeUpdate();
                if (month >= 0 && key.isMonthlyStats()) {
                    smt.setInt(3, month);
                    smt.executeUpdate();
                }
                if (day >= 0 && key.isDailyStats()) {
                    smt.setInt(3, day);
                    smt.executeUpdate();
                }
                updateResult.setNewValues(Math.min(or(updateResult.getOldAlltime(), Integer.MAX_VALUE), value), Math.min(or(updateResult.getOldMonth(), Integer.MAX_VALUE), value), Math.min(or(updateResult.getOldDay(), Integer.MAX_VALUE), value));
                return updateResult;
            }
        });
    }

    public boolean deleteScore(int databaseId, StatisticKeyImplementation key) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Boolean>() {
            @Override
            public Boolean execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(deleteScore);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                return smt.executeUpdate() > 0;
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
                return rv;
            }
        });
    }

    public Integer getPositionMax(int databaseId, StatisticKeyImplementation key, int month) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getPositionDescending);
                smt.setInt(1, keyId);
                smt.setInt(2, month);
                smt.setInt(3, databaseId);
                smt.setInt(4, keyId);
                smt.setInt(5, month);
                ResultSet results = smt.executeQuery();
                Integer rv = null;
                if (results.next()) {
                    rv = results.getInt("count");
                }
                results.close();
                return (rv == null ? 0 : rv) + 1;
            }
        });
    }

    public Integer getPositionMin(int databaseId, StatisticKeyImplementation key, int month) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getPositionAscending);
                smt.setInt(1, keyId);
                smt.setInt(2, month);
                smt.setInt(3, databaseId);
                smt.setInt(4, keyId);
                smt.setInt(5, month);
                ResultSet results = smt.executeQuery();
                Integer rv = null;
                if (results.next()) {
                    rv = results.getInt("count");
                }
                results.close();
                return (rv == null ? 0 : rv) + 1;
            }
        });
    }

    public Integer getPositionMaxTotalOrder(int databaseId, StatisticKeyImplementation key, int month) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getPositionMaxTotalOrder);
                // zeile 1
                smt.setInt(1, keyId);
                smt.setInt(2, month);
                // zeile 2
                smt.setInt(3, databaseId);
                smt.setInt(4, keyId);
                smt.setInt(5, month);
                // zeile 3
                smt.setInt(6, databaseId);
                smt.setInt(7, keyId);
                smt.setInt(8, month);
                // zeile 4
                smt.setInt(9, databaseId);
                smt.setInt(10, keyId);
                smt.setInt(11, month);
                smt.setInt(12, databaseId);
                // zeile 5
                smt.setInt(13, keyId);
                smt.setInt(14, month);
                // zeile 6
                smt.setInt(15, databaseId);
                smt.setInt(16, keyId);
                smt.setInt(17, month);

                ResultSet results = smt.executeQuery();
                Integer rv = null;
                if (results.next()) {
                    rv = results.getInt("count");
                }
                results.close();
                // System.out.println("getPositionMaxTotalOrder (" + databaseId + ") -> " + ((rv == null ? 0 : rv) + 1));
                return (rv == null ? 0 : rv) + 1;
            }
        });
    }

    public Integer getPositionMinTotalOrder(int databaseId, StatisticKeyImplementation key, int month) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getPositionMinTotalOrder);
                // zeile 1
                smt.setInt(1, keyId);
                smt.setInt(2, month);
                // zeile 2
                smt.setInt(3, databaseId);
                smt.setInt(4, keyId);
                smt.setInt(5, month);
                // zeile 3
                smt.setInt(6, databaseId);
                smt.setInt(7, keyId);
                smt.setInt(8, month);
                // zeile 4
                smt.setInt(9, databaseId);
                smt.setInt(10, keyId);
                smt.setInt(11, month);
                smt.setInt(12, databaseId);
                // zeile 5
                smt.setInt(13, keyId);
                smt.setInt(14, month);
                // zeile 6
                smt.setInt(15, databaseId);
                smt.setInt(16, keyId);
                smt.setInt(17, month);

                ResultSet results = smt.executeQuery();
                Integer rv = null;
                if (results.next()) {
                    rv = results.getInt("count");
                }
                results.close();
                // System.out.println("getPositionMinTotalOrder (" + databaseId + ") -> " + ((rv == null ? 0 : rv) + 1));
                return (rv == null ? 0 : rv) + 1;
            }
        });
    }

    public int getScoreEntries(StatisticKeyImplementation key, int month) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getScoreEntries);
                smt.setInt(1, keyId);
                smt.setInt(2, month);
                ResultSet results = smt.executeQuery();
                int result = 0;
                if (results.next()) {
                    result = results.getInt(1);
                }
                results.close();
                return result;
            }
        });
    }

    public List<InternalPlayerWithScore> getTop(StatisticKeyImplementation key, int start, int count, Ordering order, int month, PositionAlgorithm positionAlgorithm, Ordering positionOrder) throws SQLException {
        Preconditions.checkArgument(start >= 0, "start must be >= 0, but is %s", start);
        Preconditions.checkArgument(count >= 0, "count must be >= 0, but is %s", count);
        return this.connection.runCommands(new SQLRunnable<List<InternalPlayerWithScore>>() {
            @Override
            public List<InternalPlayerWithScore> execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                PreparedStatement smt = sqlConnection.getOrCreateStatement(order == Ordering.DESCENDING ? getTopScoresDesc : getTopScoresAsc);
                smt.setInt(1, keyId);
                smt.setInt(2, month);
                smt.setInt(3, start);
                smt.setInt(4, count);
                ResultSet results = smt.executeQuery();
                ArrayList<InternalPlayerWithScore> rv = new ArrayList<>();
                int position = start + 1;
                int totalPosition = start + 1;
                boolean first = true;
                int previousScore = 0;
                while (results.next()) {
                    UUID player = UUID.fromString(results.getString("uuid"));
                    int score = results.getInt("score");
                    if (positionAlgorithm == PositionAlgorithm.TOTAL_ORDER) {
                        if (first && order != positionOrder) {
                            // to get the total position of the first entry we have to get the total count of entries
                            PreparedStatement smt2 = sqlConnection.getOrCreateStatement(getScoreEntries);
                            smt2.setInt(1, keyId);
                            smt2.setInt(2, month);
                            ResultSet results2 = smt2.executeQuery();
                            int totalEntries = 0;
                            if (results2.next()) {
                                totalEntries = results2.getInt(1);
                            }
                            results2.close();
                            position = (totalEntries - start);
                            totalPosition = position;
                        }
                        position = totalPosition;
                    } else if (positionAlgorithm == PositionAlgorithm.SKIP_POSITIONS_AFTER_DUPLICATES) {
                        if (first && order == positionOrder) {
                            // if order != positionOrder we have to recalculate all positions later (start from the last entry)
                            PreparedStatement smt2 = sqlConnection.getOrCreateStatement(positionOrder == Ordering.DESCENDING ? getPositionDescendingFromScore : getPositionAscendingFromScore);
                            smt2.setInt(1, keyId);
                            smt2.setInt(2, month);
                            smt2.setInt(3, score);
                            ResultSet results2 = smt2.executeQuery();
                            if (results2.next()) {
                                position = results2.getInt(1) + 1;
                            }
                            results2.close();
                        } else if (score != previousScore) {
                            position = totalPosition;
                        }
                    } else if (positionAlgorithm == PositionAlgorithm.DO_NOT_SKIP_POSITIONS_AFTER_DUPLICATES) {
                        if (first) {
                            PreparedStatement smt2 = sqlConnection.getOrCreateStatement(positionOrder == Ordering.DESCENDING ? getPositionDescendingFromDistinctScore : getPositionAscendingFromDistinctScore);
                            smt2.setInt(1, keyId);
                            smt2.setInt(2, month);
                            smt2.setInt(3, score);
                            ResultSet results2 = smt2.executeQuery();
                            if (results2.next()) {
                                position = results2.getInt(1) + 1;
                            }
                            results2.close();
                        } else if (score != previousScore) {
                            position += positionOrder == order ? 1 : -1;
                        }
                    } else {
                        throw new RuntimeException("Unknown positionAlgorithm: " + positionAlgorithm);
                    }
                    first = false;
                    rv.add(new InternalPlayerWithScore(player, score, position));
                    previousScore = score;
                    totalPosition += positionOrder == order ? 1 : -1;
                }
                results.close();
                if (order != positionOrder && positionAlgorithm == PositionAlgorithm.SKIP_POSITIONS_AFTER_DUPLICATES && !rv.isEmpty()) {
                    PreparedStatement smt2 = sqlConnection.getOrCreateStatement(getScoreEntries);
                    smt2.setInt(1, keyId);
                    smt2.setInt(2, month);
                    ResultSet results2 = smt2.executeQuery();
                    int totalEntries = 0;
                    if (results2.next()) {
                        totalEntries = results2.getInt(1);
                    }
                    results2.close();
                    totalPosition = (totalEntries - start) - rv.size() + 1;

                    InternalPlayerWithScore lastEntry = rv.get(rv.size() - 1);
                    smt2 = sqlConnection.getOrCreateStatement(positionOrder == Ordering.DESCENDING ? getPositionDescendingFromScore : getPositionAscendingFromScore);
                    smt2.setInt(1, keyId);
                    smt2.setInt(2, month);
                    smt2.setInt(3, lastEntry.getScore());
                    results2 = smt2.executeQuery();
                    if (results2.next()) {
                        position = results2.getInt(1) + 1;
                    }
                    results2.close();
                    previousScore = lastEntry.getScore();
                    for (int i = rv.size() - 1; i >= 0; i--) {
                        InternalPlayerWithScore oldEntry = rv.get(i);
                        int score = oldEntry.getScore();
                        if (previousScore != score) {
                            position = totalPosition;
                            previousScore = score;
                        }
                        rv.set(i, new InternalPlayerWithScore(oldEntry.getPlayer(), score, position));
                        totalPosition += 1;
                    }
                }
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

    public SettingKeyImplementation createSettingKey(String name) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<SettingKeyImplementation>() {
            @Override
            public SettingKeyImplementation execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(createSettingKey, Statement.RETURN_GENERATED_KEYS);
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
                return new SettingKeyImplementation(id, name, null, impl);
            }
        });
    }

    public void updateSettingKey(SettingKeyImplementation impl) throws SQLException {
        this.connection.runCommands(new SQLRunnable<Void>() {
            @Override
            public Void execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                PreparedStatement smt = sqlConnection.getOrCreateStatement(updateSettingKey);
                smt.setString(1, impl.getSerializedProperties());
                smt.setInt(2, impl.getId());
                smt.executeUpdate();
                internalIncreaseConfigSerial(connection, sqlConnection);
                return null;
            }
        });
    }

    public Integer getSettingValue(int databaseId, SettingKeyImplementation key) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                return internalGetSettingValue(databaseId, sqlConnection, keyId);
            }
        });
    }

    public Integer setSettingValue(int databaseId, SettingKeyImplementation key, int value, boolean queryOld) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<Integer>() {
            @Override
            public Integer execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                int keyId = key.getId();
                Integer oldLevel = queryOld ? internalGetSettingValue(databaseId, sqlConnection, keyId) : null;
                PreparedStatement smt = sqlConnection.getOrCreateStatement(setSettingValue);
                smt.setInt(1, databaseId);
                smt.setInt(2, keyId);
                smt.setInt(3, value);
                smt.setInt(4, value);
                smt.executeUpdate();
                return oldLevel;
            }
        });
    }

    protected Integer internalGetSettingValue(int databaseId, SQLConnection sqlConnection, int keyId) throws SQLException {
        PreparedStatement smt = sqlConnection.getOrCreateStatement(getSettingValue);
        smt.setInt(1, databaseId);
        smt.setInt(2, keyId);
        ResultSet results = smt.executeQuery();
        Integer rv = null;
        if (results.next()) {
            rv = results.getInt("value");
        }
        results.close();
        return rv == null ? 0 : rv;
    }

    public HashMap<SettingKeyImplementation, Integer> getSettingValues(int databaseId, Collection<SettingKeyImplementation> keys) throws SQLException {
        return this.connection.runCommands(new SQLRunnable<HashMap<SettingKeyImplementation, Integer>>() {
            @Override
            public HashMap<SettingKeyImplementation, Integer> execute(Connection connection, SQLConnection sqlConnection) throws SQLException {
                HashMap<SettingKeyImplementation, Integer> result = new HashMap<>();
                HashMap<Integer, SettingKeyImplementation> tempKeyMap = new HashMap<>();
                for (SettingKeyImplementation key : keys) {
                    tempKeyMap.put(key.getId(), key);
                }
                PreparedStatement smt = sqlConnection.getOrCreateStatement(getSettingValuesPlayer);
                smt.setInt(1, databaseId);
                ResultSet results = smt.executeQuery();
                while (results.next()) {
                    SettingKeyImplementation key = tempKeyMap.get(results.getInt("settingid"));
                    int value = results.getInt("value");
                    if (key != null) {
                        result.put(key, value);
                    }
                }
                results.close();
                return result;
            }
        });
    }

    public static class StatsUpdateResultDTO {
        private final Integer oldAlltime;
        private final Integer oldMonth;
        private final Integer oldDay;
        private int newAlltime;
        private int newMonth;
        private int newDay;

        public StatsUpdateResultDTO(Integer oldAlltime, Integer oldMonth, Integer oldDay) {
            this.oldAlltime = oldAlltime;
            this.oldMonth = oldMonth;
            this.oldDay = oldDay;
        }

        protected void setNewValues(int newAlltime, int newMonth, int newDay) {
            this.newAlltime = newAlltime;
            this.newMonth = newMonth;
            this.newDay = newDay;
        }

        public Integer getOldAlltime() {
            return oldAlltime;
        }

        public Integer getOldMonth() {
            return oldMonth;
        }

        public Integer getOldDay() {
            return oldDay;
        }

        public int getNewAlltime() {
            return newAlltime;
        }

        public int getNewMonth() {
            return newMonth;
        }

        public int getNewDay() {
            return newDay;
        }
    }
}
