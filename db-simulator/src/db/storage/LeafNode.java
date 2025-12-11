package db.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import db.metrics.BTreeMetrics;
import db.storage.row.Row;

public class LeafNode implements DiskPage {
    private static long pageIdCounter = 0;

    private final long pageId;
    LeafNode leftPtr;
    LeafNode rightPtr;
    List<Row> rowData;
    private int maxRows = 5;

    public LeafNode() {
        this.pageId = ++pageIdCounter;
        this.rowData = new ArrayList<>();
        leftPtr = null;
        rightPtr = null;
    }

    public long getPageId() {
        return pageId;
    }

    @Override
    public SplitResult insertRow(Row.Key key, Row.Value value) {
        System.out.println(
                "\n[LEAF NODE] Attempting to insert row with key=" + key.key() + ", value=\"" + value.value() + "\"");

        // Every insert modifies the page, so it needs to be written back to disk
        BTreeMetrics.getInstance().recordDiskWrite();

        Row newRow = new Row(key, value);

        int insertPosition = Collections.binarySearch(rowData, newRow,
                Comparator.comparingInt(row -> row.rowKey().key()));
        // I have assumed that we won't be adding
        // duplicate keys
        if (insertPosition >= 0) {
            throw new IllegalArgumentException("duplicates are not supported");
        } else {
            insertPosition = -(insertPosition + 1);
        }

        rowData.add(insertPosition, newRow);
        System.out.println("[LEAF NODE] ✓ Row inserted at position " + insertPosition + " in leaf node");
        System.out.println("[LEAF NODE] Current leaf contains " + rowData.size() + " rows (max=" + maxRows + ")");

        boolean isSplitNeeded = isSplitNeeded();

        if (!isSplitNeeded) {
            System.out.println("[LEAF NODE] No split needed. Leaf node has space.");
            return null;
        }

        System.out.println("[LEAF NODE] ⚠ SPLIT REQUIRED! Leaf node exceeded capacity.");
        return split();

    }

    private boolean isSplitNeeded() {
        return rowData.size() > maxRows;
    }

    private SplitResult split() {
        System.out.println("[LEAF NODE] === Beginning leaf split operation ===");
        BTreeMetrics.getInstance().recordLeafSplit();

        LeafNode newLeafNode = new LeafNode();
        // New page created, needs to be written to disk
        BTreeMetrics.getInstance().recordDiskWrite();
        int fromIndex = rowData.size() / 2;
        int toIndex = rowData.size();

        moveDataToNewLeafNode(newLeafNode, fromIndex, toIndex);

        adjustPointers(newLeafNode, this);

        // Separator key is the first key of the new right leaf
        Row.Key separatorKey = newLeafNode.rowData.get(0).rowKey();
        System.out.println("[LEAF NODE] Separator key for parent: " + separatorKey.key());
        System.out.println("[LEAF NODE] === Leaf split complete ===");
        return new SplitResult(separatorKey, newLeafNode);
    }

    private void moveDataToNewLeafNode(LeafNode newLeafNode, int fromIndex, int toIndex) {
        List<Row> rowsToMove = new ArrayList<>(rowData.subList(fromIndex, toIndex));
        newLeafNode.rowData.addAll(rowsToMove);
        rowData.subList(fromIndex, toIndex).clear();
    }

    private void adjustPointers(LeafNode newLeafNode, LeafNode currentLeafNode) {
        System.out.println("[LEAF NODE] Adjusting doubly-linked list pointers between leaf nodes");
        newLeafNode.rightPtr = currentLeafNode.rightPtr;
        if (newLeafNode.rightPtr != null) {
            newLeafNode.rightPtr.leftPtr = newLeafNode;
        }
        currentLeafNode.rightPtr = newLeafNode;
        newLeafNode.leftPtr = currentLeafNode;
    }

}
