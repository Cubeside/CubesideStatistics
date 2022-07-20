package de.iani.cubesidestats;

import org.bukkit.configuration.ConfigurationSection;

public class SQLConfig {
    private String host = "localhost";

    private String user = "CHANGETHIS";

    private String password = "CHANGETHIS";

    private String database = "CHANGETHIS";

    private String tableprefix = "cubeside_settings";

    private boolean checkTables;

    public SQLConfig(ConfigurationSection section) {
        this(section, true);
    }

    public SQLConfig(ConfigurationSection section, boolean checkTables) {
        if (section != null) {
            host = section.getString("host", host);
            user = section.getString("user", user);
            password = section.getString("password", password);
            database = section.getString("database", database);
            tableprefix = section.getString("tableprefix", tableprefix);
        }
        this.checkTables = checkTables;
    }

    public String getHost() {
        return host;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getTablePrefix() {
        return tableprefix;
    }

    public boolean isCheckTables() {
        return checkTables;
    }
}
