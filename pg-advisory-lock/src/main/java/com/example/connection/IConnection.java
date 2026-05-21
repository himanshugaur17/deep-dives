package com.example.connection;

public interface IConnection extends AutoCloseable {
    void executeQuery(String query);

    void commit();

    void rollback();

    void beginTransaction();
}
