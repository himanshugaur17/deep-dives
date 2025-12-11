package db.metrics;

/**
 * Immutable snapshot of B+Tree split statistics.
 */
public record SplitStatistics(long leafSplits, long internalSplits, long totalInsertions) {

    public long totalSplits() {
        return leafSplits + internalSplits;
    }

    public double splitsPerInsertion() {
        return totalInsertions == 0 ? 0.0 : (double) totalSplits() / totalInsertions;
    }

    public double leafSplitRatio() {
        long total = totalSplits();
        return total == 0 ? 0.0 : (double) leafSplits / total;
    }

    public void printReport() {
        System.out.println("\n=== B+Tree Split Statistics ===");
        System.out.println("Total Insertions:    " + totalInsertions);
        System.out.println("Leaf Splits:         " + leafSplits);
        System.out.println("Internal Splits:     " + internalSplits);
        System.out.println("Total Splits:        " + totalSplits());
        System.out.println("---");
        System.out.printf("Splits per Insertion: %.3f\n", splitsPerInsertion());
        System.out.printf("Leaf Split Ratio:     %.1f%%\n", leafSplitRatio() * 100);
        System.out.printf("Internal Split Ratio: %.1f%%\n", (1 - leafSplitRatio()) * 100);
        System.out.println("================================\n");
    }
}
