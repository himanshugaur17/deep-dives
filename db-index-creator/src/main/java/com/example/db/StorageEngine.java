package com.example.db;

import com.example.db.models.Row;

public interface StorageEngine {
    void save(String key, Row row);
    Row load(String key);
} 
