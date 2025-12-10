package db.storage.engine;

import db.storage.row.Row;
import db.storage.row.Row.Key;

public interface DbEngine {
    Row insertRow(Key key, String value);

    Row search(Key key);
}
