package com.example.db.initializer;

import com.example.db.conn.mgr.DbConnectionMgr;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.stream.Collectors;

public class DatabaseInitializer {

    private final DbConnectionMgr connectionMgr;

    public DatabaseInitializer(DbConnectionMgr connectionMgr) {
        this.connectionMgr = connectionMgr;
    }

    public void initialize() {
        System.out.println("Initializing database...");
        executeSchemaScript();
        executeDataScript();
        System.out.println("Database initialization completed successfully!");
    }

    private void executeSchemaScript() {
        System.out.println("Executing schema.sql...");
        executeSqlFile("db/schema.sql");
    }

    private void executeDataScript() {
        System.out.println("Executing data.sql...");
        executeSqlFile("db/data.sql");
    }

    private void executeSqlFile(String filePath) {
        try (Connection conn = connectionMgr.getConnection()) {
            if (conn == null) {
                throw new RuntimeException("Failed to get database connection");
            }

            String sqlContent = readSqlFile(filePath);
            System.out.println("Read SQL file: " + filePath + " (" + sqlContent.length() + " characters)");

            // Split by semicolon to execute each statement separately
            String[] statements = sqlContent.split(";");
            System.out.println("Found " + statements.length + " SQL statements");

            try (Statement stmt = conn.createStatement()) {
                int executed = 0;
                for (String sql : statements) {
                    // Remove comments and trim
                    String[] lines = sql.split("\n");
                    StringBuilder cleanSql = new StringBuilder();
                    for (String line : lines) {
                        line = line.trim();
                        if (!line.startsWith("--") && !line.isEmpty()) {
                            cleanSql.append(line).append(" ");
                        }
                    }

                    String finalSql = cleanSql.toString().trim();
                    if (!finalSql.isEmpty()) {
                        System.out.println("Executing statement " + (executed + 1) + ": " + finalSql.substring(0, Math.min(50, finalSql.length())) + "...");
                        stmt.execute(finalSql);
                        executed++;
                    }
                }
                System.out.println("Executed " + executed + " statements from " + filePath);
            }

            System.out.println("Successfully executed: " + filePath);

        } catch (Exception e) {
            System.err.println("Error executing SQL file: " + filePath);
            e.printStackTrace();
            throw new RuntimeException("Failed to execute SQL file: " + filePath, e);
        }
    }

    private String readSqlFile(String filePath) {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath)) {
            if (inputStream == null) {
                throw new RuntimeException("SQL file not found: " + filePath);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        } catch (Exception e) {
            System.err.println("Error reading SQL file: " + filePath);
            e.printStackTrace();
            throw new RuntimeException("Failed to read SQL file: " + filePath, e);
        }
    }

    public void resetDatabase() {
        System.out.println("Resetting database...");
        dropTables();
        initialize();
    }

    private void dropTables() {
        System.out.println("Dropping existing tables...");
        try (Connection conn = connectionMgr.getConnection()) {
            if (conn == null) {
                System.err.println("Failed to get connection for dropping tables, skipping...");
                return;
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS seats CASCADE");
                stmt.execute("DROP TABLE IF EXISTS users CASCADE");
                System.out.println("Tables dropped successfully");
            }

        } catch (Exception e) {
            System.err.println("Error dropping tables (may not exist yet, continuing...)");
            // Don't print full stack trace, just continue
        }
    }
}