package db.storage.row;

public record Row(Key rowKey, Value rowValue) {
    public record Key(int key) {
    }

    public record Value(String value) {
    }
}
