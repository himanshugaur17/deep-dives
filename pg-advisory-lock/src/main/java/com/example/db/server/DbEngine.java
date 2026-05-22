package com.example.db.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.example.connection.IConnection;
import com.example.db.LockMode;
import com.example.db.server.locking.LockMgr;
import com.example.db.server.locking.LockRegistry;
import com.example.db.server.locking.LockStrategy;
import com.example.db.server.locking.SessionLockingStrategy;

public class DbEngine implements LockRegistry, LockMgr {

    private final Map<LockMode, LockStrategy> lockStrategies = Map.of(LockMode.TRANSACTION, null, LockMode.SESSION,
            new SessionLockingStrategy());
    private final Map<String, String> activeTxnLocks = new ConcurrentHashMap<>();
    private final Map<String, String> activeSessionLocks = new ConcurrentHashMap<>();

    String checkDbHealth() {
        return "OK";
    }

    @Override
    public boolean acquireLock(IConnection connection, LockMode lockMode, String lockKey) {
        LockStrategy strategy = lockStrategies.get(lockMode);
        if (strategy == null) {
            throw new UnsupportedOperationException("Unsupported lock mode: " + lockMode);
        }
        return strategy.acquire(connection, lockKey, this, lockMode);
    }

    @Override
    public boolean releaseLock(IConnection connection, String lockKey) {
        LockStrategy strategy = lockStrategies.get(LockMode.SESSION);
        if (strategy == null) {
            throw new UnsupportedOperationException("Unsupported lock mode: " + LockMode.SESSION);
        }
        return strategy.release(connection, lockKey, this);
    }

    @Override
    public boolean tryRegisterTxnLock(String lockKey) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'tryRegisterTxnLock'");
    }

    @Override
    public boolean tryRegisterSessionLock(String lockKey) {
        return activeSessionLocks.putIfAbsent(lockKey, lockKey) == null;
    }

    @Override
    public boolean tryUnregisterTxnLock(String lockKey) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'tryUnregisterTxnLock'");
    }

    @Override
    public boolean tryUnregisterSessionLock(String lockKey) {
        return activeSessionLocks.remove(lockKey) != null;
    }
}
