package com.example.db.server.locking;

import com.example.connection.IConnection;
import com.example.db.LockMode;

public interface LockRegistry {
    boolean acquireLock(IConnection connection, LockMode lockMode, String lockKey);

    boolean releaseLock(IConnection connection, String lockKey);
}
