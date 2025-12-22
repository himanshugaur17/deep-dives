package com.example.db.conn.mgr;

import java.util.Properties;

public class DbConnectionMgr {

    private Properties dbProperties;

    public DbConnectionMgr() {
        loadDbProperties();
    }

    private void loadDbProperties() {
        dbProperties = new Properties();
        try (var input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                dbProperties.load(input);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public java.sql.Connection getConnection() {
        try {
            String url = dbProperties.getProperty("db.url");
            String user = dbProperties.getProperty("db.user");
            String password = dbProperties.getProperty("db.password");
            return java.sql.DriverManager.getConnection(url, user, password);
        } catch (java.sql.SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
