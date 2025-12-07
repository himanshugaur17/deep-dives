#!/usr/bin/env python3
import time
import psycopg2

# Config
HOST = "localhost"
PORT = 5432
DATABASE = "postgres"
USER = "postgres"
PASSWORD = "postgres"
ITERATIONS = 100
SIMULATED_RTT_MS = 5

def run_single_query(rtt_ms):
    """Run one query and measure connection vs query time."""
    rtt_sec = rtt_ms / 1000.0

    # I have assumed 1.5 RTT for TCP handshake
    # 2RTT for postgres auth (username and password based)
    start = time.perf_counter()
    if rtt_ms > 0:
        time.sleep(3.5 * rtt_sec)
    conn = psycopg2.connect(host=HOST, port=PORT, database=DATABASE, user=USER, password=PASSWORD)
    cursor = conn.cursor()
    setup_time = time.perf_counter() - start

    # Query execution (1 RTT)
    start = time.perf_counter()
    if rtt_ms > 0:
        time.sleep(1.0 * rtt_sec)
    cursor.execute("SELECT 1")
    cursor.fetchone()
    query_time = time.perf_counter() - start

    # Connection teardown (2 RTT)
    start = time.perf_counter()
    if rtt_ms > 0:
        time.sleep(2.0 * rtt_sec)
    cursor.close()
    conn.close()
    teardown_time = time.perf_counter() - start

    return setup_time, query_time, teardown_time

# Run benchmark
print(f"\nRunning {ITERATIONS} iterations with {SIMULATED_RTT_MS}ms RTT...\n")

setup_times = []
query_times = []
teardown_times = []

for i in range(ITERATIONS):
    setup, query, teardown = run_single_query(SIMULATED_RTT_MS)
    setup_times.append(setup)
    query_times.append(query)
    teardown_times.append(teardown)

# Calculate averages
avg_setup = sum(setup_times) / len(setup_times) * 1000
avg_query = sum(query_times) / len(query_times) * 1000
avg_teardown = sum(teardown_times) / len(teardown_times) * 1000
total = avg_setup + avg_query + avg_teardown

# Print results
print(f"Connection Setup:    {avg_setup:.2f}ms  ({avg_setup/total*100:.1f}%)")
print(f"Query Execution:     {avg_query:.2f}ms  ({avg_query/total*100:.1f}%)")
print(f"Connection Teardown: {avg_teardown:.2f}ms  ({avg_teardown/total*100:.1f}%)")
print(f"Total:               {total:.2f}ms")
print(f"\nConnection overhead is {(avg_setup + avg_teardown) / avg_query:.1f}x the query time!")
