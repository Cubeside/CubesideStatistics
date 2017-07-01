package de.iani.cubesidestats;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import de.iani.settings.sql.MySQLConnection;
import de.iani.settings.sql.SQLConnection;
import de.iani.settings.sql.SQLRunnable;

public class StatisticsDatabase {
    private SQLConnection connection;
    private final CubesideStatistics plugin;

    private final String getConfigValue;
    private final String increaseConfigValue;

    private final String getPlayerId;
    private final String createPlayerId;

    private final String getAllStatsKeys;
    private final String createStatsKey;
    private final String updateStatsKey;

    public StatisticsDatabase(CubesideStatistics plugin, SQLConfig config) throws SQLException {
        this.plugin = plugin;
        connection = new MySQLConnection(config.getHost(), config.getDatabase(), config.getUser(), config.getPassword());
        String prefix = config.getTablePrefix();
        updateTables(prefix);

        getConfigValue = "SELECT value FROM " + prefix + "_config WHERE setting = ?";
        increaseConfigValue = "UPDATE " + prefix + "_config set value = value + 1 WHERE setting = ?";
        getPlayerId = "SELECT id FROM " + prefix + "_players WHERE uuid = ?";
        createPlayerId = "INSERT INTO " + prefix + "_players (uuid) VALUE (?)";

        getAllStatsKeys = "SELECT id, name, properties FROM " + prefix + "_stats";
        createStatsKey = "INSERT INTO " + prefix + "_stats (name, properties) VALUE (?, ?)";
        updateStatsKey = "UPDATE " + prefix + "_stats SET properties = ? WHERE id = ?";
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
}
