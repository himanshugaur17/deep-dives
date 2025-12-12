package db.core.in.memory.buffer;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import db.metrics.BTreeMetrics;

public class BufferPool {
    private static final BufferPool INSTANCE = new BufferPool();
    private static Integer SIZE = 10;
    private LinkedHashSet<Long> pagesInBuffer = new LinkedHashSet<>();
    private Set<Long> dirtyPages = new HashSet<>(); // Pages modified in buffer
    private Set<Long> newPages = new HashSet<>(); // Pages not yet persisted to disk

    public static BufferPool getInstance() {
        return INSTANCE;
    }

    public static void setBufferPoolSize(int size) {
        SIZE = size;
    }

    private BufferPool() {

    }

    public void tryReadingPageFromMemory(Long pageId) {
        if (pagesInBuffer.contains(pageId))
            return;
        BTreeMetrics.getInstance().recordDiskRead();
        pagesInBuffer.add(pageId);
        System.out.println("[BUFFER] Miss! Reading disk page " + pageId + " from disk");
        evictIfNeeded();
    }

    public void addNewPageToBuffer(Long pageId) {
        pagesInBuffer.add(pageId);
        newPages.add(pageId);
        evictIfNeeded();
    }

    public void markPageDirty(Long pageId) {
        dirtyPages.add(pageId);
    }

    private void evictIfNeeded() {
        if (pagesInBuffer.size() > SIZE) {
            Long evictedPage = pagesInBuffer.removeFirst();

            if (dirtyPages.contains(evictedPage) || newPages.contains(evictedPage)) {
                BTreeMetrics.getInstance().recordDiskWrite();
                dirtyPages.remove(evictedPage);
            }

            newPages.remove(evictedPage);
        }
    }

    public void flush() {
        // Write all dirty and new pages remaining in buffer to disk
        for (Long pageId : pagesInBuffer) {
            if (dirtyPages.contains(pageId) || newPages.contains(pageId)) {
                BTreeMetrics.getInstance().recordDiskWrite();
            }
        }
        dirtyPages.clear();
        newPages.clear();
    }

    public void reset() {
        pagesInBuffer.clear();
        dirtyPages.clear();
        newPages.clear();
    }
}
