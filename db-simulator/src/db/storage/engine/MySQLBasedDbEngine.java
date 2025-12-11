package db.storage.engine;

import db.metrics.BTreeMetrics;
import db.storage.DiskPage;
import db.storage.InternalNode;
import db.storage.LeafNode;
import db.storage.SplitResult;
import db.storage.row.Row;

public class MySQLBasedDbEngine implements DbEngine {
    private DiskPage rootPage;
    private Long lastAccessedLeafPageId = null;

    public MySQLBasedDbEngine() {
        this.rootPage = new LeafNode();
    }

    @Override
    public Row insertRow(Row.Key key, String value) {
        BTreeMetrics.getInstance().recordInsertion();

        // Find target leaf page before insertion
        LeafNode targetLeaf = findLeafForKey(rootPage, key);

        // Simulate buffer pool: check if page is in memory
        if (lastAccessedLeafPageId == null || lastAccessedLeafPageId != targetLeaf.getPageId()) {
            // Buffer miss - need to read page from disk
            BTreeMetrics.getInstance().recordDiskRead();
            System.out.println("[BUFFER] Miss! Reading leaf page " + targetLeaf.getPageId() + " from disk");
            lastAccessedLeafPageId = targetLeaf.getPageId();
        } else {
            System.out.println("[BUFFER] Hit! Leaf page " + targetLeaf.getPageId() + " already in memory");
        }

        SplitResult splitResult = rootPage.insertRow(key, new Row.Value(value));

        if (splitResult != null) {
            DiskPage newRoot = InternalNode.createAsRoot(rootPage, splitResult.getNewPage(),
                    splitResult.getSeparatorKey());
            System.out.println("[ENGINE] Root split! New root created. Tree height increased.");
            rootPage = newRoot;
            // Root changed, invalidate buffer cache
            lastAccessedLeafPageId = null;
        }

        return new Row(key, new Row.Value(value));
    }

    private LeafNode findLeafForKey(DiskPage page, Row.Key key) {
        if (page instanceof LeafNode) {
            return (LeafNode) page;
        }

        // Traverse internal nodes to find the target leaf
        InternalNode internalNode = (InternalNode) page;
        DiskPage child = internalNode.findChildForKey(key);
        return findLeafForKey(child, key);
    }

}
