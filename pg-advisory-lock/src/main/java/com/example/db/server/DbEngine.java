package com.example.db.server;

import com.example.connection.IConnection;
import com.example.db.LockMode;
import com.example.db.server.locking.LockRegistry;

public class DbEngine implements LockRegistry {

    String checkDbHealth() {
        return "OK";
    }

    @Override
    public boolean acquireLock(IConnection connection, LockMode lockMode, String lockKey) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'acquireLock'");
    }

    @Override
    public boolean releaseLock(IConnection connection, String lockKey) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'releaseLock'");
    }
}
