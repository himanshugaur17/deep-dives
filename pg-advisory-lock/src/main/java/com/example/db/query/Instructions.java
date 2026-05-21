package com.example.db.query;

import com.example.connection.IConnection;

@FunctionalInterface
public interface Instructions {
    void instructions(IConnection connection);
}
