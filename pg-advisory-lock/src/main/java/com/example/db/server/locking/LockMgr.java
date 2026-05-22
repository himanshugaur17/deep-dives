package com.example.db.server.locking;

public interface LockMgr {
    boolean tryRegisterTxnLock(String lockKey);

    boolean tryRegisterSessionLock(String lockKey);

    boolean tryUnregisterTxnLock(String lockKey);

    boolean tryUnregisterSessionLock(String lockKey);
}
