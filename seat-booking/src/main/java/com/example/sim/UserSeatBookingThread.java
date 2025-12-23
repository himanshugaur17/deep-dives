package com.example.sim;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.example.db.conn.mgr.DbConnectionMgr;
import com.example.locking.strategy.SQLQueryLockingStrategy;

public class UserSeatBookingThread extends Thread {

    private final DbConnectionMgr dbConnectionMgr;
    private final int userId;
    private final SQLQueryLockingStrategy lockingStrategy;
    private boolean bookingSuccessful = false;
    private String bookedSeatNumber = null;
    private long bookingTimeMs = 0;
    private String errorMessage = null;

    public UserSeatBookingThread(DbConnectionMgr dbConnectionMgr, int userId, SQLQueryLockingStrategy lockingStrategy) {
        this.dbConnectionMgr = dbConnectionMgr;
        this.userId = userId;
        this.lockingStrategy = lockingStrategy;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();

        try (Connection connection = dbConnectionMgr.getConnection()) {
            if (connection == null) {
                errorMessage = "Failed to get database connection";
                return;
            }

            // Start transaction
            connection.setAutoCommit(false);

            try {
                // Find first available seat using the configured locking strategy
                String sqlQuery = lockingStrategy.getQuery();

                String seatNumber = null;
                try (PreparedStatement selectStatement = connection.prepareStatement(sqlQuery);
                        ResultSet rs = selectStatement.executeQuery()) {

                    if (rs.next()) {
                        seatNumber = rs.getString("seat_number");
                    }
                }

                if (seatNumber != null) {
                    // Book the seat
                    String updateQuery = "UPDATE seats SET user_id = ? WHERE seat_number = ?";
                    try (PreparedStatement updateStmt = connection.prepareStatement(updateQuery)) {
                        updateStmt.setInt(1, userId);
                        updateStmt.setString(2, seatNumber);
                        int rowsUpdated = updateStmt.executeUpdate();

                        if (rowsUpdated > 0) {
                            connection.commit();
                            bookingSuccessful = true;
                            bookedSeatNumber = seatNumber;
                            bookingTimeMs = System.currentTimeMillis() - startTime;
                        } else {
                            connection.rollback();
                            errorMessage = "Failed to book seat " + seatNumber + " (already booked by another user)";
                            System.out.println(
                                    "User " + userId + " failed to book seat " + seatNumber + ": already booked.");
                        }
                    }
                } else {
                    connection.rollback();
                    errorMessage = "No available seats";
                }

            } catch (SQLException e) {
                connection.rollback();
                String sqlState = e.getSQLState();
                errorMessage = "Error during booking - SQLState: " + sqlState + ", Message: " + e.getMessage();

                System.err.println("[User " + userId + "] SQLException occurred:");
                System.err.println("  SQLState: " + sqlState);
                System.err.println("  Error Code: " + e.getErrorCode());
                System.err.println("  Message: " + e.getMessage());

                if ("40001".equals(sqlState) || "40P01".equals(sqlState)) {
                    System.err.println("  >>> SERIALIZATION/DEADLOCK ERROR DETECTED <<<");
                }
            } catch (Exception e) {
                connection.rollback();
                errorMessage = "Error during booking - " + e.getMessage();
                System.err.println("[User " + userId + "] Exception: " + e.getMessage());
            }

        } catch (Exception e) {
            errorMessage = "Connection error - " + e.getMessage();
        }

        if (!bookingSuccessful) {
            bookingTimeMs = System.currentTimeMillis() - startTime;
        }
    }

    public boolean isBookingSuccessful() {
        return bookingSuccessful;
    }

    public String getBookedSeatNumber() {
        return bookedSeatNumber;
    }

    public long getBookingTimeMs() {
        return bookingTimeMs;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public SQLQueryLockingStrategy getLockingStrategy() {
        return lockingStrategy;
    }
}
