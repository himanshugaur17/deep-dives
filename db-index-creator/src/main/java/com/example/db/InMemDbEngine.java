package com.example.db;

import com.example.db.models.Row;

public class InMemDbEngine implements StorageEngine {
    public InMemDbEngine() {
        // Initialize in-memory data structures if needed
    }
    @Override
    public void save(String key, Row row) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'save'");
    }

    @Override
    public Row load(String key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'load'");
    }

}
