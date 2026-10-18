package com.example.mvvacuum.scenario;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class ConcurrentRefreshScenario {

    private final JdbcTemplate jdbcTemplate;

    public ConcurrentRefreshScenario(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int changeAllFourLogicalRows() {
        return jdbcTemplate.update("""
                UPDATE demo_feature_state
                   SET enabled = NOT enabled,
                       modified_at = clock_timestamp()
                """);
    }

    public void refreshSnapshotConcurrently() {
        jdbcTemplate.execute("""
                REFRESH MATERIALIZED VIEW CONCURRENTLY demo_feature_snapshot
                """);
    }
}
