# TCP Connection Overhead Benchmark

Quick prototype to show that TCP connection setup/teardown takes way more time than the actual query.

## Setup

```bash
# Install dependency
pip install psycopg2-binary

# Start postgres
docker-compose up -d

# Run benchmark
python main.py
```

## How it works

Uses `SELECT 1` (no table needed) and measures:
- Connection setup: ~3.5 RTT (TCP handshake + auth)
- Query execution: 1 RTT
- Connection teardown: 2 RTT

Change `SIMULATED_RTT_MS` in main.py to 10 or 50 to simulate network latency.

## Cleanup

```bash
docker-compose down
```
