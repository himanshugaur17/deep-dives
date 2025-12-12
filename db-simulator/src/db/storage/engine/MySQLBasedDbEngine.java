package db.storage.engine;

import db.core.in.memory.buffer.BufferPool;
import db.metrics.BTreeMetrics;
import db.storage.DiskPage;
import db.storage.InternalNode;
import db.storage.LeafNode;
import db.storage.SplitResult;
import db.storage.row.Row;

public class MySQLBasedDbEngine implements DbEngine {
    private DiskPage rootPage;

    public MySQLBasedDbEngine() {
        this.rootPage = new LeafNode();
    }

    @Override
    public Row insertRow(Row.Key key, String value) {
        BTreeMetrics.getInstance().recordInsertion();
        BufferPool.getInstance().tryReadingPageFromMemory(rootPage.getPageId());
        SplitResult splitResult = rootPage.insertRow(key, new Row.Value(value));
        if (splitResult != null) {
            DiskPage newRootPage = InternalNode.createAsRoot(rootPage, splitResult.getNewPage(),
                    splitResult.getSeparatorKey());
            System.out.println("[ENGINE] Root split! New root created. Tree height increased.");
            rootPage = newRootPage;

        }
        return new Row(key, new Row.Value(value));
    }

}
