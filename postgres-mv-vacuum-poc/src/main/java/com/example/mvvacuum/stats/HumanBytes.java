package com.example.mvvacuum.stats;

import java.util.Locale;

public final class HumanBytes {

    private static final String[] UNITS = {"B", "KiB", "MiB", "GiB", "TiB"};

    private HumanBytes() {
    }

    public static String format(long bytes) {
        double value = bytes;
        int unitIndex = 0;

        while (value >= 1024 && unitIndex < UNITS.length - 1) {
            value /= 1024;
            unitIndex++;
        }

        if (unitIndex == 0) {
            return bytes + " B";
        }
        return String.format(Locale.ROOT, "%.1f %s", value, UNITS[unitIndex]);
    }
}
