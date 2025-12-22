package com.example.visualizer;

import com.example.db.conn.mgr.DbConnectionMgr;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class SeatMapVisualizer {

    private final DbConnectionMgr connectionMgr;

    public SeatMapVisualizer(DbConnectionMgr connectionMgr) {
        this.connectionMgr = connectionMgr;
    }

    public void displaySeatMap() {
        Map<String, Integer> seatAllocations = fetchSeatAllocations();

        System.out.println("\n" + "=".repeat(80));
        System.out.println("                           SEAT ALLOCATION MAP");
        System.out.println("=".repeat(80));
        System.out.println();
        System.out.println("Legend: [XXX] = User ID | [---] = Available Seat");
        System.out.println();

        // Display seats in a grid: 25 rows x 4 columns (A, B, C, D)
        for (int row = 1; row <= 25; row++) {
            System.out.printf("Row %2d:  ", row);

            for (char col = 'A'; col <= 'D'; col++) {
                String seatNumber = row + String.valueOf(col);
                Integer userId = seatAllocations.get(seatNumber);

                if (userId != null) {
                    System.out.printf("[%3d] ", userId);
                } else {
                    System.out.print("[---] ");
                }
            }
            System.out.println();
        }

        System.out.println();
        displayStatistics(seatAllocations);
    }

    private Map<String, Integer> fetchSeatAllocations() {
        Map<String, Integer> allocations = new HashMap<>();

        String query = "SELECT seat_number, user_id FROM seats ORDER BY seat_number";

        try (Connection conn = connectionMgr.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String seatNumber = rs.getString("seat_number");
                Integer userId = rs.getObject("user_id", Integer.class);
                allocations.put(seatNumber, userId);
            }

        } catch (Exception e) {
            System.err.println("Error fetching seat allocations: " + e.getMessage());
        }

        return allocations;
    }

    private void displayStatistics(Map<String, Integer> seatAllocations) {
        int totalSeats = seatAllocations.size();
        long bookedSeats = seatAllocations.values().stream().filter(userId -> userId != null).count();
        long availableSeats = totalSeats - bookedSeats;

        System.out.println("=".repeat(80));
        System.out.println("STATISTICS:");
        System.out.println("-".repeat(80));
        System.out.printf("Total Seats:     %d%n", totalSeats);
        System.out.printf("Booked Seats:    %d (%.1f%%)%n", bookedSeats, (bookedSeats * 100.0 / totalSeats));
        System.out.printf("Available Seats: %d (%.1f%%)%n", availableSeats, (availableSeats * 100.0 / totalSeats));
        System.out.println("=".repeat(80));
    }
}
