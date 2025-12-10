package db.storage;

import java.util.ArrayList;
import java.util.List;

import db.storage.row.Row;

public class InternalNode implements DiskPage {
    private int maxKeys = 2;
    private final List<Row.Key> keys;
    private final List<DiskPage> children;

    public InternalNode() {
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
    }

    @Override
    public SplitResult insertRow(Row.Key key, Row.Value value) {
        // TODO: Implement InternalNode insertion logic
        throw new UnsupportedOperationException("Unimplemented method 'insertRow'");
    }

}
