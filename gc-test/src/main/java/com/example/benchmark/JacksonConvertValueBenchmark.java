package com.example.benchmark;

import java.util.Map;

public class JacksonConvertValueBenchmark {
    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();
    private static final int ITERATIONS = 1_000_000;
    private final Map<String, Object> INPUT_MAP = Map.of("userInfo", Map.of(
            "name", "John Doe",
            "age", 30,
            "email", "john.doe@example.com"));

    public void runBenchmark() {
        convertUsingJackson();
        useSimpleCast();
    }

    private void convertUsingJackson() {
        System.gc();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Starting Jackson convertValue benchmark...");
        long gcCountBefore = getGarbageCollectionCount();
        long startTime = System.currentTimeMillis();
        long memoryBefore = getUsedMemory();
        for (int i = 0; i < ITERATIONS; i++) {
            Map<String, Object> result = OBJECT_MAPPER.convertValue(INPUT_MAP, Map.class);
        }
        long endMemory = getUsedMemory();
        long endTime = System.currentTimeMillis();
        System.gc();
        long gcCountAfter = getGarbageCollectionCount();
        System.out.println("Jackson convertValue benchmark completed.");
        System.out.println("Time taken (ms): " + (endTime - startTime));
        System.out.println("Memory used (KB): " + (endMemory - memoryBefore) / 1024);
        System.out.println("Garbage Collections: " + (gcCountAfter - gcCountBefore));

    }

    private void useSimpleCast() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Starting Simple Cast benchmark...");
        long gcCountBefore = getGarbageCollectionCount();
        long startTime = System.currentTimeMillis();
        long memoryBefore = getUsedMemory();
        for (int i = 0; i < ITERATIONS; i++) {
            Map<String, Object> result = (Map<String, Object>) INPUT_MAP;
        }
        long endMemory = getUsedMemory();
        long endTime = System.currentTimeMillis();
        System.gc();
        long gcCountAfter = getGarbageCollectionCount();
        System.out.println("Simple Cast benchmark completed.");
        System.out.println("Time taken (ms): " + (endTime - startTime));
        System.out.println("Memory used (KB): " + (endMemory - memoryBefore) / 1024);
        System.out.println("Garbage Collections: " + (gcCountAfter - gcCountBefore));
    }

    private long getGarbageCollectionCount() {
        return java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()
                .stream()
                .mapToLong(gc -> gc.getCollectionCount())
                .sum();
    }

    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

}
