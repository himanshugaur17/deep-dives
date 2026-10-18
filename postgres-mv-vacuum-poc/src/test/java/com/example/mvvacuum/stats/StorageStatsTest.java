package com.example.mvvacuum.stats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StorageStatsTest {

    @Test
    void calculatesObsoletePhysicalVersions() {
        StorageStats stats = new StorageStats(4, 16, 16_384, 24_576, 32_768, 57_344);

        assertThat(stats.obsoletePhysicalVersions()).isEqualTo(12);
    }

    @Test
    void obsoleteVersionCountCannotBeNegative() {
        StorageStats stats = new StorageStats(4, 0, 0, 0, 0, 0);

        assertThat(stats.obsoletePhysicalVersions()).isZero();
    }

    @Test
    void formatsBytesForConsoleOutput() {
        assertThat(HumanBytes.format(512)).isEqualTo("512 B");
        assertThat(HumanBytes.format(1_536)).isEqualTo("1.5 KiB");
        assertThat(HumanBytes.format(1_048_576)).isEqualTo("1.0 MiB");
    }
}
