CREATE EXTENSION IF NOT EXISTS pgstattuple;
CREATE EXTENSION IF NOT EXISTS pageinspect;

DROP MATERIALIZED VIEW IF EXISTS demo_feature_snapshot;
DROP TABLE IF EXISTS demo_feature_state;
DROP TABLE IF EXISTS demo_feature;
DROP TABLE IF EXISTS demo_account;

CREATE TABLE demo_account (
    account_id BIGINT PRIMARY KEY,
    display_name TEXT NOT NULL
);

CREATE TABLE demo_feature (
    feature_id INTEGER PRIMARY KEY,
    feature_name TEXT NOT NULL
);

CREATE TABLE demo_feature_state (
    state_id BIGINT PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES demo_account(account_id),
    feature_id INTEGER NOT NULL REFERENCES demo_feature(feature_id),
    enabled BOOLEAN NOT NULL,
    modified_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    details TEXT NOT NULL,
    UNIQUE (account_id, feature_id)
);

INSERT INTO demo_account (account_id, display_name)
VALUES
    (10, 'Demo Account Alpha'),
    (20, 'Demo Account Beta');

INSERT INTO demo_feature (feature_id, feature_name)
VALUES
    (1, 'Demo Feature One'),
    (2, 'Demo Feature Two');

-- Each details value is about 1 KiB and intentionally hard to compress. This
-- makes heap growth visible with only four live rows and a few refresh cycles.
INSERT INTO demo_feature_state (
    state_id,
    account_id,
    feature_id,
    enabled,
    details
)
SELECT
    seed.state_id,
    seed.account_id,
    seed.feature_id,
    seed.enabled,
    (
        SELECT string_agg(
            md5(seed.state_id::text || '-' || part_number::text),
            '' ORDER BY part_number
        )
        FROM generate_series(1, 32) AS part_number
    )
FROM (
    VALUES
        (101::BIGINT, 10::BIGINT, 1, TRUE),
        (102::BIGINT, 10::BIGINT, 2, TRUE),
        (201::BIGINT, 20::BIGINT, 1, TRUE),
        (202::BIGINT, 20::BIGINT, 2, FALSE)
) AS seed(state_id, account_id, feature_id, enabled);

CREATE MATERIALIZED VIEW demo_feature_snapshot
WITH (fillfactor = 100)
AS
SELECT
    state.state_id,
    state.account_id,
    account.display_name AS account_name,
    state.feature_id,
    feature.feature_name,
    state.enabled,
    state.modified_at,
    state.details
FROM demo_feature_state AS state
JOIN demo_account AS account
  ON account.account_id = state.account_id
JOIN demo_feature AS feature
  ON feature.feature_id = state.feature_id
WITH DATA;

-- A qualifying unique index is required by REFRESH ... CONCURRENTLY.
CREATE UNIQUE INDEX demo_feature_snapshot_state_uq
    ON demo_feature_snapshot (state_id);

CREATE INDEX demo_feature_snapshot_account_idx
    ON demo_feature_snapshot (account_id);

-- Stage one deliberately disables autovacuum so dead tuples remain available
-- for observation. A later scenario will enable and study vacuum separately.
ALTER MATERIALIZED VIEW demo_feature_snapshot
    SET (autovacuum_enabled = false);

ANALYZE demo_feature_snapshot;
