package com.example.db.server.locking;

import com.example.connection.IConnection;
import com.example.db.LockMode;

public class SessionLockingStrategy implements LockStrategy {
    @Override
    public boolean acquire(IConnection connection, String lockKey, LockMgr lockMgr, LockMode lockMode) {
        return lockMgr.tryRegisterSessionLock(lockKey);
    }

    @Override
    public boolean release(IConnection connection, String lockKey, LockMgr lockMgr) {
        return lockMgr.tryUnregisterSessionLock(lockKey);
    }

}
