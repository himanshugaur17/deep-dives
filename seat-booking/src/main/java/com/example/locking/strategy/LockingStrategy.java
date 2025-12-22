package com.example.locking.strategy;

public enum LockingStrategy {

    /**
     * Use SELECT FOR UPDATE - Acquires row-level lock and waits if row is already locked
     */
    WITH_LOCK("SELECT seat_number FROM seats WHERE user_id IS NULL ORDER BY seat_number LIMIT 1 FOR UPDATE"),

    /**
     * Use SELECT without any locking - No lock acquisition, may cause race conditions
     */
    NO_LOCK("SELECT seat_number FROM seats WHERE user_id IS NULL ORDER BY seat_number LIMIT 1"),

    /**
     * Use SELECT FOR UPDATE SKIP LOCKED - Skips rows that are already locked by other transactions
     */
    SKIP_LOCKED("SELECT seat_number FROM seats WHERE user_id IS NULL ORDER BY seat_number LIMIT 1 FOR UPDATE SKIP LOCKED");

    private final String query;

    LockingStrategy(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public String getDescription() {
        return switch (this) {
            case WITH_LOCK -> "SELECT FOR UPDATE (waits for lock)";
            case NO_LOCK -> "SELECT without locking (no lock)";
            case SKIP_LOCKED -> "SELECT FOR UPDATE SKIP LOCKED (skips locked rows)";
        };
    }
}