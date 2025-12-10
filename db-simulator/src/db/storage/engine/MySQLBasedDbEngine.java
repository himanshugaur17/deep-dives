package db.storage.engine;

import db.storage.row.Row;

public class MySQLBasedDbEngine implements DbEngine {
    @Override
    public Row insertRow(Row.Key key, String value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'insertRow'");
    }

    @Override
    public Row search(Row.Key key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'search'");
    }

}
