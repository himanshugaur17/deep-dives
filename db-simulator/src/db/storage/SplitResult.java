package db.storage;

import db.storage.row.Row;

public class SplitResult {
    private final Row.Key separatorKey;
    private final DiskPage newPage;

    public SplitResult(Row.Key separatorKey, DiskPage newPage) {
        this.separatorKey = separatorKey;
        this.newPage = newPage;
    }

    public Row.Key getSeparatorKey() {
        return separatorKey;
    }

    public DiskPage getNewPage() {
        return newPage;
    }
}
