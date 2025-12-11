package db.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Singleton to track B+Tree split operations.
 */
public class SplitMetrics {
    private static final SplitMetrics INSTANCE = new SplitMetrics();

    private final AtomicLong leafSplits = new AtomicLong(0);
    private final AtomicLong internalSplits = new AtomicLong(0);
    private final AtomicLong insertions = new AtomicLong(0);

    private SplitMetrics() {}

    public static SplitMetrics getInstance() {
        return INSTANCE;
    }

    public void recordLeafSplit() {
        leafSplits.incrementAndGet();
    }

    public void recordInternalSplit() {
        internalSplits.incrementAndGet();
    }

    public void recordInsertion() {
        insertions.incrementAndGet();
    }

    public SplitStatistics getStatistics() {
        return new SplitStatistics(leafSplits.get(), internalSplits.get(), insertions.get());
    }

    public void reset() {
        leafSplits.set(0);
        internalSplits.set(0);
        insertions.set(0);
    }
}
