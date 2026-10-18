# PostgreSQL Materialized-View Vacuum POC

This Java/PostgreSQL deep dive demonstrates one idea at a time. Stage one shows
how dead materialized-view tuples accumulate when changed rows are repeatedly
merged by `REFRESH MATERIALIZED VIEW CONCURRENTLY` and vacuum does not clean
them.

All database objects are fictional and use a `demo_` prefix. No company schema,
table, index, view, or domain names are used.

## What stage one demonstrates

The schema contains two demo accounts and two demo features, producing four
live rows in `demo_feature_snapshot`.

For every cycle, the Java runner:

1. Changes all four rows in `demo_feature_state`.
2. Runs a concurrent materialized-view refresh.
3. Uses `pageinspect` to count physical tuple versions and prints relation sizes.

Autovacuum is disabled only on the demo materialized view so dead tuples remain
available for observation. The runner also holds a repeatable-read snapshot so
later writes cannot opportunistically prune earlier versions. This is a
deterministic laboratory control; it is not a claim that a long transaction
caused the production incident. No manual `VACUUM` is run in this stage.

Expected relationship:

```text
Cycle 0: 4 live,  4 physical versions,  0 obsolete
Cycle 1: 4 live,  8 physical versions,  4 obsolete
Cycle 2: 4 live, 12 physical versions,  8 obsolete
Cycle 3: 4 live, 16 physical versions, 12 obsolete
```

The four current-snapshot rows remain live. Older physical versions are dead or
not-yet-removable versions retained by PostgreSQL's MVCC rules. The heap or
index grows only when existing 8 KiB pages are exhausted, so version counts
change every cycle while file sizes change in page-sized jumps.

## Prerequisites

- Java 21
- Maven 3.9+
- Docker with Docker Compose

## Run it

Start PostgreSQL:

```bash
docker compose up -d --wait
```

Run the Java application:

```bash
mvn spring-boot:run
```

The application pauses before each cycle. Press Enter to run exactly one update
and concurrent refresh, or enter `q` to stop.

For an automatic 20-cycle run:

```bash
mvn -Dspring-boot.run.arguments="--app.poc.pause-between-cycles=false --app.poc.refresh-cycles=20" spring-boot:run
```

## Configuration

Defaults are in `src/main/resources/application.yml`.

```yaml
app:
  poc:
    enabled: true
    reset-schema: true
    refresh-cycles: 10
    pause-between-cycles: true
```

The database connection can be overridden with:

```text
POC_DB_URL
POC_DB_USER
POC_DB_PASSWORD
```

## Stop PostgreSQL

Preserve the Docker volume:

```bash
docker compose down
```

Delete the local POC database and its volume:

```bash
docker compose down -v
```

The `-v` command permanently deletes only this POC's Docker volume.

## Deliberately not included yet

- Running ordinary vacuum and observing reusable space
- Inspecting raw heap pages and B-tree TIDs
- Measuring a larger query before and after bloat
- Reproducing the autovacuum/concurrent-refresh lock conflict

Those should be added as separate scenarios after the stage-one output is
understood.
