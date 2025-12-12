package db.storage;

public class PageIdGenerator {
    private static final PageIdGenerator INSTANCE = new PageIdGenerator();
    private long counter = 0;

    private PageIdGenerator() {
    }

    public static PageIdGenerator getInstance() {
        return INSTANCE;
    }

    public long nextId() {
        return ++counter;
    }

    public void reset() {
        counter = 0;
    }
}
