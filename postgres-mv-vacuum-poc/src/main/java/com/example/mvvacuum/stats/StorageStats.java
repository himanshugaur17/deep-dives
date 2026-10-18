package com.example.mvvacuum.stats;

public record StorageStats(
        long liveTuples,
        long physicalTupleVersions,
        long physicalTupleBytes,
        long heapBytes,
        long indexBytes,
        long totalBytes
) {
    public long obsoletePhysicalVersions() {
        return Math.max(0, physicalTupleVersions - liveTuples);
    }
}
