package com.example.mvvacuum.scenario;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

@Component
public class PinnedSnapshot {

    private final DataSource dataSource;

    public PinnedSnapshot(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Handle open() throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            // The first SELECT establishes the repeatable-read snapshot. Holding
            // it prevents opportunistic pruning from erasing old row versions
            // while this deterministic teaching scenario is running.
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT count(*) FROM demo_feature_snapshot")) {
                resultSet.next();
                long visibleRows = resultSet.getLong(1);
                if (visibleRows != 4) {
                    throw new IllegalStateException(
                            "Expected four rows when pinning the snapshot, but found " + visibleRows);
                }
            }
            return new Handle(connection);
        } catch (Exception exception) {
            connection.close();
            throw exception;
        }
    }

    public static final class Handle implements AutoCloseable {

        private final Connection connection;

        private Handle(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws SQLException {
            try {
                connection.rollback();
            } finally {
                connection.close();
            }
        }
    }
}
