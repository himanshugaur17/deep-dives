package com.example.db.client;

import com.example.connection.Connection;
import com.example.connection.IConnection;
import com.example.db.LockMode;
import com.example.db.server.DbEngine;
import com.example.db.server.locking.LockRegistry;

public class DbClientImpl implements IDbClient {
    private final LockRegistry lockRegistry = new DbEngine();

    @Override
    public void executeWithLock(LockMode lockMode, Instructions instructions) {
        String lockKey = "my_lock_key_" + lockMode.name() + "_" + System.currentTimeMillis();
        IConnection connection = new Connection();
        boolean lockAcquired = lockRegistry.acquireLock(connection, lockMode, lockKey);
        if (lockAcquired) {
            try {
                instructions.instructions(connection);
            } finally {
                lockRegistry.releaseLock(connection, lockKey);
            }
        } else {
            System.out.println("Failed to acquire lock: " + lockKey);
        }

    }

}
