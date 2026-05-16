package com.example.db.models;

public class Row {
    private final int id;
    private String data;
    private boolean isDeleted;
    private int xminId;

    public Row(int id, String data, int xminId) {
        this.id = id;
        this.data = data;
        this.isDeleted = false;
        this.xminId = xminId;
    }

}
