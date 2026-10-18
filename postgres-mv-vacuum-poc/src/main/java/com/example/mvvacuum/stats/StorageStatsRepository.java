package com.example.mvvacuum.stats;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class StorageStatsRepository {

    private final JdbcTemplate jdbcTemplate;

    public StorageStatsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public StorageStats readExactStats() {
        return jdbcTemplate.queryForObject("""
                WITH heap_pages AS (
                    SELECT generate_series(
                        0,
                        pg_relation_size('demo_feature_snapshot'::regclass)
                            / current_setting('block_size')::integer - 1
                    ) AS block_number
                ),
                physical_versions AS (
                    SELECT
                        count(*) FILTER (
                            WHERE item.lp_flags = 1 AND item.lp_len > 0
                        ) AS tuple_versions,
                        coalesce(sum(item.lp_len) FILTER (
                            WHERE item.lp_flags = 1 AND item.lp_len > 0
                        ), 0) AS tuple_bytes
                    FROM heap_pages
                    CROSS JOIN LATERAL heap_page_items(
                        get_raw_page(
                            'demo_feature_snapshot',
                            heap_pages.block_number
                        )
                    ) AS item
                )
                SELECT
                    (SELECT count(*) FROM demo_feature_snapshot),
                    physical_versions.tuple_versions,
                    physical_versions.tuple_bytes,
                    pg_relation_size('demo_feature_snapshot'::regclass),
                    pg_indexes_size('demo_feature_snapshot'::regclass),
                    pg_total_relation_size('demo_feature_snapshot'::regclass)
                FROM physical_versions
                """, (resultSet, rowNumber) -> new StorageStats(
                resultSet.getLong(1),
                resultSet.getLong(2),
                resultSet.getLong(3),
                resultSet.getLong(4),
                resultSet.getLong(5),
                resultSet.getLong(6)));
    }
}
