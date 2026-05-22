package com.example.db.client;

import com.example.connection.IConnection;

public class UpdateQueryInstructions implements Instructions {
    @Override
    public void instructions(IConnection connection) {
        connection.beginTransaction();
        connection.executeQuery("UPDATE users SET name = 'Himanshu' WHERE id = 1");
        connection.commit();
    }

}
