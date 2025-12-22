package com.example;

import com.example.db.conn.mgr.DbConnectionMgr;
import com.example.locking.strategy.LockingStrategy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SeatBookingThread extends Thread {

    private final DbConnectionMgr connectionMgr;
    private final int userId;
    private final LockingStrategy lockingStrategy;
    private boolean bookingSuccessful = false;
    private String bookedSeatNumber = null;
    private long bookingTimeMs = 0;
    private String errorMessage = null;

    public SeatBookingThread(DbConnectionMgr connectionMgr, int userId, LockingStrategy lockingStrategy) {
        this.connectionMgr = connectionMgr;
        this.userId = userId;
        this.lockingStrategy = lockingStrategy;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();

        try (Connection conn = connectionMgr.getConnection()) {
            if (conn == null) {
                errorMessage = "Failed to get database connection";
                System.err.println("User " + userId + ": " + errorMessage);
                return;
            }

            // Start transaction
            conn.setAutoCommit(false);

            try {
                // Find first available seat using the configured locking strategy
                String selectQuery = lockingStrategy.getQuery();

                String seatNumber = null;
                try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery);
                     ResultSet rs = selectStmt.executeQuery()) {

                    if (rs.next()) {
                        seatNumber = rs.getString("seat_number");
                    }
                }

                if (seatNumber != null) {
                    // Book the seat
                    String updateQuery = "UPDATE seats SET user_id = ? WHERE seat_number = ? AND user_id IS NULL";
                    try (PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {
                        updateStmt.setInt(1, userId);
                        updateStmt.setString(2, seatNumber);
                        int rowsUpdated = updateStmt.executeUpdate();

                        if (rowsUpdated > 0) {
                            conn.commit();
                            bookingSuccessful = true;
                            bookedSeatNumber = seatNumber;
                            bookingTimeMs = System.currentTimeMillis() - startTime;
                            System.out.println("User " + userId + " successfully booked seat " + seatNumber +
                                             " using " + lockingStrategy.getDescription() + " (took " + bookingTimeMs + "ms)");
                        } else {
                            conn.rollback();
                            errorMessage = "Failed to book seat " + seatNumber + " (already booked by another user)";
                            System.out.println("User " + userId + ": " + errorMessage);
                        }
                    }
                } else {
                    conn.rollback();
                    errorMessage = "No available seats";
                    System.out.println("User " + userId + ": " + errorMessage);
                }

            } catch (Exception e) {
                conn.rollback();
                errorMessage = "Error during booking - " + e.getMessage();
                System.err.println("User " + userId + ": " + errorMessage);
                e.printStackTrace();
            }

        } catch (Exception e) {
            errorMessage = "Connection error - " + e.getMessage();
            System.err.println("User " + userId + ": " + errorMessage);
            e.printStackTrace();
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

    public LockingStrategy getLockingStrategy() {
        return lockingStrategy;
    }
}