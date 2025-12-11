package db.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import db.metrics.BTreeMetrics;
import db.storage.row.Row;

public class InternalNode implements DiskPage {
    private static final int MAX_KEYS = 3;
    private final List<Row.Key> keys;
    private final List<DiskPage> children;

    public InternalNode() {
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
    }

    public static InternalNode createAsRoot(DiskPage leftChild, DiskPage rightChild, Row.Key separator) {
        InternalNode root = new InternalNode();
        root.keys.add(separator);
        root.children.add(leftChild);
        root.children.add(rightChild);
        return root;
    }

    @Override
    public SplitResult insertRow(Row.Key key, Row.Value value) {
        int childIndex = findChildIndex(key);
        System.out.println("[INTERNAL NODE] Navigating to child at index " + childIndex + " (out of " + children.size()
                + " children)");

        DiskPage childPage = children.get(childIndex);
        String childType = childPage instanceof LeafNode ? "LEAF" : "INTERNAL";
        System.out.println("[INTERNAL NODE] Descending to " + childType + " node");

        SplitResult childSplitResult = childPage.insertRow(key, value);

        if (childSplitResult == null) {
            System.out.println("[INTERNAL NODE] Child did not split. Insert complete at this level.");
            return null;
        }

        Row.Key separator = childSplitResult.getSeparatorKey();
        DiskPage rightChild = childSplitResult.getNewPage();
        System.out.println("\n[INTERNAL NODE] Child node split! Received separator key=" + separator.key());

        insertSeparatorAndChild(separator, rightChild);

        if (keys.size() > MAX_KEYS) {
            System.out.println("[INTERNAL NODE] ⚠ INTERNAL NODE OVERFLOW! Splitting internal node...");
            return split();
        }

        System.out.println("[INTERNAL NODE] Internal node has capacity. No split needed.");
        return null;

    }

    private void insertSeparatorAndChild(Row.Key separator, DiskPage rightChild) {
        int keyPosition = findChildIndex(separator);
        keys.add(keyPosition, separator);
        children.add(keyPosition + 1, rightChild);
        System.out.println("[INTERNAL NODE] Separator key " + separator.key() + " inserted at position " + keyPosition);
    }

    private int findChildIndex(Row.Key key) {
        int insertionPoint = Collections.binarySearch(keys, key, Comparator.comparingInt(k -> k.key()));
        int childIndex = insertionPoint >= 0 ? insertionPoint + 1 : -(insertionPoint + 1);
        return childIndex;
    }

    public DiskPage findChildForKey(Row.Key key) {
        int childIndex = findChildIndex(key);
        return children.get(childIndex);
    }

    private SplitResult split() {
        System.out.println("[INTERNAL NODE] === Beginning internal node split ===");
        BTreeMetrics.getInstance().recordInternalSplit();
        int splitPoint = (keys.size() + 1) / 2;
        InternalNode rightNode = new InternalNode();

        moveDataToRightNode(rightNode, splitPoint);
        System.out.println(
                "[INTERNAL NODE] Left internal node: " + keys.size() + " keys, " + children.size() + " children");
        System.out.println("[INTERNAL NODE] Right internal node: " + rightNode.keys.size() + " keys, "
                + rightNode.children.size() + " children");

        Row.Key separator = rightNode.keys.get(0);
        System.out.println("[INTERNAL NODE] Promoting separator key=" + separator.key() + " to parent");
        System.out.println("[INTERNAL NODE] === Internal node split complete ===");
        return new SplitResult(separator, rightNode);
    }

    private void moveDataToRightNode(InternalNode rightNode, int splitPoint) {
        // Copy right half to new node
        rightNode.keys.addAll(keys.subList(splitPoint, keys.size()));
        rightNode.children.addAll(children.subList(splitPoint, children.size()));

        // Remove right half from current node
        keys.subList(splitPoint, keys.size()).clear();
        // Left node keeps splitPoint+1 children (N keys need N+1 children)
        children.subList(splitPoint + 1, children.size()).clear();
    }

}
