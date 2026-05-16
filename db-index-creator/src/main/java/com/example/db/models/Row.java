package com.example.db.models;

public class Row {
    private String key;
    private String value;
    private int xminId;
    private int xmaxId;

    public Row(String key, String value, int xminId, int xmaxId) {
        this.key = key;
        this.value = value;
        this.xminId = xminId;
        this.xmaxId = xmaxId;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getXminId() {
        return xminId;
    }

    public int getXmaxId() {
        return xmaxId;
    }
}
