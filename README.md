# Deep Dives

Real Engineering prototypes for real understanding.

## Projects

### [db-simulator](db-simulator/)
Java-based database storage engine simulator implementing B+-tree data structures.

**Key Components:**
- B-tree implementation (leaf and internal nodes)
- Disk page abstraction
- Database engine interfaces

### [heartbeats-benchmarking](heartbeats-benchmarking/)
Python-based benchmarking tools for analyzing connection overhead and heartbeat protocols.

**Current Benchmarks:**
- [TCP Connection Overhead](heartbeats-benchmarking/tcp/) - Measures TCP setup/teardown vs query execution time

### [postgres-mv-vacuum-poc](postgres-mv-vacuum-poc/)
Java and PostgreSQL POC showing how concurrent materialized-view refreshes can
accumulate dead tuples when vacuum cleanup does not run.

## Getting Started

Each project is self-contained with its own dependencies and configuration. See individual project READMEs for setup instructions.
