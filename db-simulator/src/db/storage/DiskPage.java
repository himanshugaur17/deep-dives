package db.storage;

import db.storage.row.Row;

public interface DiskPage {
    /**
     * Representation of a physical page on disk that would actually be storing
     * data, so called database.
     */
    SplitResult insertRow(Row.Key key, Row.Value value);

    Long getPageId();
}
