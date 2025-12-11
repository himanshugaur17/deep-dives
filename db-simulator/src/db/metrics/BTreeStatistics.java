package db.metrics;

public record BTreeStatistics(
    long leafSplits,
    long internalSplits,
    long insertions,
    long diskReads,
    long diskWrites
) {
    public long totalSplits() {
        return leafSplits + internalSplits;
    }

    public double splitsPerInsertion() {
        return insertions == 0 ? 0.0 : (double) totalSplits() / insertions;
    }

    public double ioPerInsertion() {
        return insertions == 0 ? 0.0 : (double) (diskReads + diskWrites) / insertions;
    }
}