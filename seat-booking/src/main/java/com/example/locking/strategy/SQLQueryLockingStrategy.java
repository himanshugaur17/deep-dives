package com.example.locking.strategy;

public enum SQLQueryLockingStrategy {
    WITH_LOCK("SELECT seat_number FROM seats WHERE user_id IS NULL ORDER BY CAST(SUBSTRING(seat_number FROM '^[0-9]+') AS INTEGER), SUBSTRING(seat_number FROM '[A-Z]+$') LIMIT 1 FOR UPDATE"),
    NO_LOCK("SELECT seat_number FROM seats WHERE user_id IS NULL ORDER BY CAST(SUBSTRING(seat_number FROM '^[0-9]+') AS INTEGER), SUBSTRING(seat_number FROM '[A-Z]+$') LIMIT 1"),
    SKIP_LOCKED("SELECT seat_number FROM seats WHERE user_id IS NULL ORDER BY CAST(SUBSTRING(seat_number FROM '^[0-9]+') AS INTEGER), SUBSTRING(seat_number FROM '[A-Z]+$') LIMIT 1 FOR UPDATE SKIP LOCKED");

    private final String sqlQuery;

    SQLQueryLockingStrategy(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    public String getQuery() {
        return sqlQuery;
    }

    public String getDescription() {
        return switch(this) {
            case WITH_LOCK -> "SELECT FOR UPDATE (waits for lock)";
            case NO_LOCK -> "SELECT without lock and then update";
            case SKIP_LOCKED -> "SELECT FOR UPDATE SKIP LOCKED (skips locked rows)";
        };
    }
}
