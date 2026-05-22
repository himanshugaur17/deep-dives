package com.example.db.client;

import com.example.db.LockMode;

public interface IDbClient {
    void executeWithLock(LockMode lockMode, Instructions instructions);

}
