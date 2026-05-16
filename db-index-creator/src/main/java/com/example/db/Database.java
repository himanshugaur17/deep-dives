package com.example.db;

import java.util.List;

import com.example.db.models.Query;
import com.example.db.models.Row;

public interface Database {
    List<Row> run(Query query);
    
}
