package db.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import db.storage.row.Row;

public class LeafNode implements DiskPage {
    LeafNode leftPtr;
    LeafNode rightPtr;
    List<Row> rowData;
    private int maxRows = 4;

    public LeafNode() {
        this.rowData = new ArrayList<>();
        leftPtr = null;
        rightPtr = null;
    }

    @Override
    public SplitResult insertRow(Row.Key key, Row.Value value) {
        Row newRow = new Row(key, value);

        int insertPosition = Collections.binarySearch(rowData, newRow,
                Comparator.comparingInt(row -> row.rowKey().key()));
        // I have assumed that we won't be adding
        // duplicate keys
        if (insertPosition >= 0)
            throw new IllegalArgumentException("duplicates are not supported");
        else
            insertPosition = -(insertPosition + 1);

        rowData.add(insertPosition, newRow);
        boolean isSplitNeeded = isSplitNeeded();

        if (!isSplitNeeded)
            return null;
        return split();

    }

    private boolean isSplitNeeded() {
        return rowData.size() > maxRows;
    }

    private SplitResult split() {
        LeafNode newLeafNode = new LeafNode();
        int fromIndex = rowData.size() / 2;
        int toIndex = rowData.size();
        moveDataToNewLeafNode(newLeafNode, fromIndex, toIndex);
        adjustPointers(newLeafNode, this);

        // Separator key is the first key of the new right leaf
        Row.Key separatorKey = newLeafNode.rowData.get(0).rowKey();
        return new SplitResult(separatorKey, newLeafNode);
    }

    private void moveDataToNewLeafNode(LeafNode newLeafNode, int fromIndex, int toIndex) {
        List<Row> rowsToMove = new ArrayList<>(rowData.subList(fromIndex, toIndex));
        newLeafNode.rowData.addAll(rowsToMove);
        rowData.subList(fromIndex, toIndex).clear();
    }

    private void adjustPointers(LeafNode newLeafNode, LeafNode currentLeafNode) {
        newLeafNode.rightPtr = currentLeafNode.rightPtr;
        if (newLeafNode.rightPtr != null) {
            newLeafNode.rightPtr.leftPtr = newLeafNode;
        }
        currentLeafNode.rightPtr = newLeafNode;
        newLeafNode.leftPtr = currentLeafNode;
    }

}
