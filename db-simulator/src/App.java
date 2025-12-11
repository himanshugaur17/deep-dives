import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import db.metrics.BTreeMetrics;
import db.storage.engine.MySQLBasedDbEngine;
import db.storage.row.Row;

public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("=== B+ Tree: Insertion Order Impact Study ===\n");
        System.out.println("Configuration: maxRows=5, MAX_KEYS=3\n");

        int numInsertions = 50;

        // Test 1: Sequential insertions
        System.out.println("Test 1: Sequential Order (1→" + numInsertions + ")");
        BTreeMetrics.getInstance().reset();
        MySQLBasedDbEngine engine1 = new MySQLBasedDbEngine();

        for (int i = 1; i <= numInsertions; i++) {
            engine1.insertRow(new Row.Key(i), "Value_" + i);
        }

        var stats1 = BTreeMetrics.getInstance().getStatistics();
        System.out.println("  Leaf Splits:     " + stats1.leafSplits());
        System.out.println("  Internal Splits: " + stats1.internalSplits());
        System.out.println("  Total Splits:    " + stats1.totalSplits());
        System.out.println("  Disk Reads:      " + stats1.diskReads());
        System.out.println("  Disk Writes:     " + stats1.diskWrites());
        System.out.println("  Total I/O:       " + (stats1.diskReads() + stats1.diskWrites()));

        // Test 2: Random insertions
        System.out.println("\nTest 2: Random Order");
        BTreeMetrics.getInstance().reset();
        MySQLBasedDbEngine engine2 = new MySQLBasedDbEngine();

        List<Integer> randomKeys = new ArrayList<>();
        for (int i = 1; i <= numInsertions; i++) {
            randomKeys.add(i);
        }
        Collections.shuffle(randomKeys);
        System.out.println("  Order: " + randomKeys.subList(0, Math.min(10, randomKeys.size())) + "...");

        for (int key : randomKeys) {
            engine2.insertRow(new Row.Key(key), "Value_" + key);
        }

        var stats2 = BTreeMetrics.getInstance().getStatistics();
        System.out.println("  Leaf Splits:     " + stats2.leafSplits());
        System.out.println("  Internal Splits: " + stats2.internalSplits());
        System.out.println("  Total Splits:    " + stats2.totalSplits());
        System.out.println("  Disk Reads:      " + stats2.diskReads());
        System.out.println("  Disk Writes:     " + stats2.diskWrites());
        System.out.println("  Total I/O:       " + (stats2.diskReads() + stats2.diskWrites()));

        // Comparison
        System.out.println("\n=== COMPARISON ===");
        System.out.println("Split Operations:");
        System.out.println("  Sequential: " + stats1.totalSplits() + " splits");
        System.out.println("  Random:     " + stats2.totalSplits() + " splits");

        System.out.println("\nDisk I/O Operations:");
        System.out.println("  Sequential: " + stats1.diskReads() + " reads, " + stats1.diskWrites() + " writes = "
                + (stats1.diskReads() + stats1.diskWrites()) + " total");
        System.out.println("  Random:     " + stats2.diskReads() + " reads, " + stats2.diskWrites() + " writes = "
                + (stats2.diskReads() + stats2.diskWrites()) + " total");

        long seqIO = stats1.diskReads() + stats1.diskWrites();
        long randIO = stats2.diskReads() + stats2.diskWrites();
        double ioIncrease = ((double) randIO / seqIO - 1) * 100;

        System.out.println("\n✓ Impact: Random inserts cause " + String.format("%.1f%%", ioIncrease)
                + " MORE disk I/O than sequential!");
        System.out.println("  This is why sequential inserts are faster in MySQL.");
    }
}
