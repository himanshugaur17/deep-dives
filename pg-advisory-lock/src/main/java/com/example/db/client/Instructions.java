package com.example.db.client;

import com.example.connection.IConnection;

@FunctionalInterface
public interface Instructions {
    void instructions(IConnection connection);
}
