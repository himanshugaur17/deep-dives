package com.example.db.server.locking;

import com.example.connection.IConnection;
import com.example.db.LockMode;

public interface LockStrategy {
    boolean acquire(IConnection connection, String lockKey, LockMgr lockMgr, LockMode lockMode);

    boolean release(IConnection connection, String lockKey, LockMgr lockMgr);
}
