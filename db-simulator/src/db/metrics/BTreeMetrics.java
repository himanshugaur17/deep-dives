package db.metrics;

/**
 * Singleton to track B+Tree operations: splits and disk I/O.
 */
public class BTreeMetrics {
    private static final BTreeMetrics INSTANCE = new BTreeMetrics();

    private long leafSplits = 0;
    private long internalSplits = 0;
    private long insertions = 0;
    private long diskReads = 0;
    private long diskWrites = 0;

    private BTreeMetrics() {}

    public static BTreeMetrics getInstance() {
        return INSTANCE;
    }

    public void recordLeafSplit() {
        leafSplits++;
    }

    public void recordInternalSplit() {
        internalSplits++;
    }

    public void recordInsertion() {
        insertions++;
    }

    public void recordDiskRead() {
        diskReads++;
    }

    public void recordDiskWrite() {
        diskWrites++;
    }

    public BTreeStatistics getStatistics() {
        return new BTreeStatistics(
            leafSplits,
            internalSplits,
            insertions,
            diskReads,
            diskWrites
        );
    }

    public void reset() {
        leafSplits = 0;
        internalSplits = 0;
        insertions = 0;
        diskReads = 0;
        diskWrites = 0;
    }
}