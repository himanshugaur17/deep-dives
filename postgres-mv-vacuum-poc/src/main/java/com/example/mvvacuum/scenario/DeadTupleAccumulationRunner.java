package com.example.mvvacuum.scenario;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.example.mvvacuum.config.PocProperties;
import com.example.mvvacuum.stats.HumanBytes;
import com.example.mvvacuum.stats.StorageStats;
import com.example.mvvacuum.stats.StorageStatsRepository;

@Component
public class DeadTupleAccumulationRunner implements ApplicationRunner {

    private static final String TABLE_HEADER = """
            Cycle | Live | Physical versions | Obsolete versions | Tuple bytes | Heap     | Indexes  | Total
            ------+------+-------------------+-------------------+-------------+----------+----------+----------""";

    private final PocProperties properties;
    private final DemoSchemaManager schemaManager;
    private final ConcurrentRefreshScenario refreshScenario;
    private final StorageStatsRepository statsRepository;
    private final PinnedSnapshot pinnedSnapshot;
    private final BufferedReader input;

    public DeadTupleAccumulationRunner(
            PocProperties properties,
            DemoSchemaManager schemaManager,
            ConcurrentRefreshScenario refreshScenario,
            StorageStatsRepository statsRepository,
            PinnedSnapshot pinnedSnapshot
    ) {
        this.properties = properties;
        this.schemaManager = schemaManager;
        this.refreshScenario = refreshScenario;
        this.statsRepository = statsRepository;
        this.pinnedSnapshot = pinnedSnapshot;
        this.input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    }

    @Override
    public void run(ApplicationArguments args) throws IOException, SQLException {
        if (!properties.enabled()) {
            System.out.println("POC runner is disabled.");
            return;
        }

        if (properties.resetSchema()) {
            System.out.println("Resetting the fictional demo schema...");
            schemaManager.reset();
        }

        System.out.println("""

                Stage 1: accumulate dead materialized-view tuples
                --------------------------------------------------
                Logical data remains constant at four rows.
                Each cycle changes all four source rows, then runs:
                  REFRESH MATERIALIZED VIEW CONCURRENTLY demo_feature_snapshot

                Autovacuum is disabled only for this demo materialized view.
                A repeatable-read snapshot is pinned so old physical versions
                cannot be opportunistically pruned during this experiment.
                """);
        System.out.println(TABLE_HEADER);
        printStats(0, statsRepository.readExactStats());

        try (PinnedSnapshot.Handle ignored = pinnedSnapshot.open()) {
            boolean inputReachedEnd = false;
            for (int cycle = 1; cycle <= properties.refreshCycles(); cycle++) {
                if (properties.pauseBetweenCycles() && !inputReachedEnd) {
                    System.out.printf(
                            "%nPress ENTER for cycle %d/%d, or type q and ENTER to stop: ",
                            cycle,
                            properties.refreshCycles());
                    String response = input.readLine();
                    if (response == null) {
                        inputReachedEnd = true;
                        System.out.println("Input is not interactive; continuing automatically.");
                    } else if (response.trim().equalsIgnoreCase("q")) {
                        System.out.println("Stopped before the next refresh.");
                        break;
                    }
                }

                int changedRows = refreshScenario.changeAllFourLogicalRows();
                refreshScenario.refreshSnapshotConcurrently();

                if (changedRows != 4) {
                    throw new IllegalStateException(
                            "Expected to change exactly four demo rows, but changed " + changedRows);
                }

                printStats(cycle, statsRepository.readExactStats());
            }
        }

        System.out.println("""

                What to notice:
                  - Live stays at 4.
                  - Physical versions increase by 4 per completed cycle.
                  - Everything above the four live versions is obsolete storage.
                  - Heap/index files grow only when their existing pages become full.

                No VACUUM was run. The pinned snapshot is now released.
                """);
    }

    private static void printStats(int cycle, StorageStats stats) {
        System.out.printf(
                "%5d | %4d | %17d | %17d | %11s | %8s | %8s | %8s%n",
                cycle,
                stats.liveTuples(),
                stats.physicalTupleVersions(),
                stats.obsoletePhysicalVersions(),
                HumanBytes.format(stats.physicalTupleBytes()),
                HumanBytes.format(stats.heapBytes()),
                HumanBytes.format(stats.indexBytes()),
                HumanBytes.format(stats.totalBytes()));
    }
}
