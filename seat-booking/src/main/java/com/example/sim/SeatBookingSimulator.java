package com.example.sim;

import java.util.ArrayList;
import java.util.List;

import com.example.db.conn.mgr.DbConnectionMgr;
import com.example.db.initializer.DatabaseInitializer;
import com.example.locking.strategy.SQLQueryLockingStrategy;
import com.example.visualizer.SeatMapVisualizer;

public class SeatBookingSimulator {

    public static void main(String[] args) throws InterruptedException {
        DbConnectionMgr connectionMgr = new DbConnectionMgr();
        DatabaseInitializer databaseInitializer = new DatabaseInitializer(connectionMgr);

        System.out.println("=== Seat Booking Simulator Started ===\n");

        // Initialize database with fresh data
        databaseInitializer.resetDatabase();

        System.out.println("\n=== Choose a locking strategy to simulate ===");
        System.out.println("1. WITH_LOCK - SELECT FOR UPDATE (waits for lock)");
        System.out.println("2. NO_LOCK - SELECT without locking");
        System.out.println("3. SKIP_LOCKED - SELECT FOR UPDATE SKIP LOCKED\n");

        SQLQueryLockingStrategy strategy = SQLQueryLockingStrategy.WITH_LOCK;

        System.out.println("Running simulation with strategy: " + strategy.getDescription() + "\n");

        runSimulation(connectionMgr, strategy);
    }

    private static void runSimulation(DbConnectionMgr connectionMgr, SQLQueryLockingStrategy strategy)
            throws InterruptedException {
        int numberOfUsers = 100;
        List<UserSeatBookingThread> threads = new ArrayList<>();

        long simulationStartTime = System.currentTimeMillis();

        // Create and start 100 threads
        System.out.println("Creating " + numberOfUsers + " booking threads...");
        for (int i = 1; i <= numberOfUsers; i++) {
            UserSeatBookingThread thread = new UserSeatBookingThread(connectionMgr, i, strategy);
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to complete
        System.out.println("Waiting for all booking threads to complete...");
        for (UserSeatBookingThread thread : threads) {
            thread.join();
        }

        long simulationEndTime = System.currentTimeMillis();
        long totalSimulationTime = simulationEndTime - simulationStartTime;

        // Print statistics
        printStatistics(threads, totalSimulationTime);

        // Display graphical seat map
        SeatMapVisualizer visualizer = new SeatMapVisualizer(connectionMgr);
        visualizer.displaySeatMap();
    }

    private static void printStatistics(List<UserSeatBookingThread> threads, long totalSimulationTime) {
        int successfulBookings = 0;
        int failedBookings = 0;
        long totalBookingTime = 0;
        long minBookingTime = Long.MAX_VALUE;
        long maxBookingTime = 0;

        for (UserSeatBookingThread thread : threads) {
            if (thread.isBookingSuccessful()) {
                successfulBookings++;
                long bookingTime = thread.getBookingTimeMs();
                totalBookingTime += bookingTime;
                minBookingTime = Math.min(minBookingTime, bookingTime);
                maxBookingTime = Math.max(maxBookingTime, bookingTime);
            } else {
                failedBookings++;
            }
        }

        System.out.println("\n=== Simulation Results ===");
        System.out.println("Locking Strategy: " + threads.get(0).getLockingStrategy().getDescription());
        System.out.println("Total Simulation Time: " + totalSimulationTime + "ms");
        System.out.println("Successful Bookings: " + successfulBookings);
        System.out.println("Failed Bookings: " + failedBookings);

        if (successfulBookings > 0) {
            long avgBookingTime = totalBookingTime / successfulBookings;
            System.out.println("Average Booking Time: " + avgBookingTime + "ms");
            System.out.println("Min Booking Time: " + minBookingTime + "ms");
            System.out.println("Max Booking Time: " + maxBookingTime + "ms");
        }

        System.out.println("\nThroughput: " + (successfulBookings * 1000.0 / totalSimulationTime) + " bookings/second");
    }
}
