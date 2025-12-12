package db.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import db.core.in.memory.buffer.BufferPool;
import db.metrics.BTreeMetrics;
import db.storage.row.Row;

public class InternalNode implements DiskPage {
    private static int MAX_KEYS = 3;
    private final long pageId;
    private final List<Row.Key> keys;
    private final List<DiskPage> children;

    public static void setMaxKeys(int max) {
        MAX_KEYS = max;
    }

    public InternalNode() {
        this.pageId = PageIdGenerator.getInstance().nextId();
        // New page created in memory - add to buffer but don't count as disk read
        BufferPool.getInstance().addNewPageToBuffer(this.pageId);
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
    public Long getPageId() {
        return pageId;
    }

    @Override
    public SplitResult insertRow(Row.Key key, Row.Value value) {
        int childIndex = findChildIndex(key);
        DiskPage childPage = children.get(childIndex);
        // Accessing child page requires reading it from disk if not in buffer
        BufferPool.getInstance().tryReadingPageFromMemory(childPage.getPageId());
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

        // Page has been modified - mark as dirty for write-back on eviction
        BufferPool.getInstance().markPageDirty(this.pageId);
    }

    private int findChildIndex(Row.Key key) {
        int insertionPoint = Collections.binarySearch(keys, key, Comparator.comparingInt(k -> k.key()));
        int childIndex = insertionPoint >= 0 ? insertionPoint + 1 : -(insertionPoint + 1);
        return childIndex;
    }

    private SplitResult split() {
        BTreeMetrics.getInstance().recordInternalSplit();
        int middleIndex = keys.size() / 2;
        Row.Key separator = keys.get(middleIndex);

        InternalNode rightNode = new InternalNode();

        moveKeysToNewNode(middleIndex, rightNode);

        return new SplitResult(separator, rightNode);
    }

    private void moveKeysToNewNode(int middleIndex, InternalNode rightNode) {
        rightNode.keys.addAll(keys.subList(middleIndex + 1, keys.size()));
        rightNode.children.addAll(children.subList(middleIndex + 1, children.size()));
        keys.subList(middleIndex, keys.size()).clear();
        children.subList(middleIndex + 1, children.size()).clear();
    }

}
