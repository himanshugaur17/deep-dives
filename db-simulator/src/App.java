import db.storage.LeafNode;
import db.storage.row.Row;

public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("DATABASE STORAGE ENGINE SIMULATOR - B+ Tree Implementation");
        System.out.println("=".repeat(80));

        // Start with a simple leaf node
        System.out.println("\n[DB ENGINE] Initializing B+ Tree with root as a leaf node");
        LeafNode root = new LeafNode();
        System.out.println("[DB ENGINE] Root node created. Max rows per leaf: 4");

        // Insert some rows to demonstrate the flow
        System.out.println("\n" + "=".repeat(80));
        System.out.println("PHASE 1: Inserting rows into the B+ Tree");
        System.out.println("=".repeat(80));

        insertRow(root, 10, "Alice");
        insertRow(root, 20, "Bob");
        insertRow(root, 15, "Charlie");
        insertRow(root, 25, "David");

        System.out.println("\n" + "=".repeat(80));
        System.out.println("PHASE 2: Triggering first leaf split (5th row)");
        System.out.println("=".repeat(80));
        insertRow(root, 5, "Eve");

        System.out.println("\n" + "=".repeat(80));
        System.out.println("DATABASE SIMULATION COMPLETE");
        System.out.println("=".repeat(80));
    }

    private static void insertRow(LeafNode root, int key, String value) {
        System.out.println("\n>>> [DB ENGINE] Client request: INSERT key=" + key + ", value=\"" + value + "\"");
        Row.Key rowKey = new Row.Key(key);
        Row.Value rowValue = new Row.Value(value);
        root.insertRow(rowKey, rowValue);
        System.out.println("<<< [DB ENGINE] Insert operation completed for key=" + key);
    }
}
