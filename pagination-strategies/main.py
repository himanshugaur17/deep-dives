from data_seeding.seed import seed_data
import time
import matplotlib.pyplot as plt
from pymongo import MongoClient, ASCENDING

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME="pagingation_test"
COLLECTION_NAME="products"
PAGE_SIZE = 100
MAX_PAGES_TO_FETCH = 500

client=MongoClient(MONGO_URI)
db=client[DB_NAME]
collection=db[COLLECTION_NAME]

def benchmark_offset_pagination_time(filter_query, sort_field):
    times=[]
    print(f"Running offset pagination for query: {filter_query} and sort_field: {sort_field}")
    for page in range(MAX_PAGES_TO_FETCH):
        start_time=time.time()
        
        skip=page*PAGE_SIZE
        cursor=collection.find(filter_query).sort(sort_field, ASCENDING).skip(skip).limit(PAGE_SIZE)
        _=list(cursor)
        end_time=time.time()

        times.append((end_time-start_time)*1000)
    return times

def benchmark_cursor_pagination_time(filter_query, sort_field):
    times=[]
    print(f"Running cursor pagination for query: {filter_query} and sort_field: {sort_field}")
    last_val=None

    for _ in range(MAX_PAGES_TO_FETCH):
        start_time=time.time()

        current_filter = filter_query.copy()
        if last_val:
            current_filter[sort_field]={"$gt":last_val}
        cursor=collection.find(current_filter).sort(sort_field, ASCENDING).limit(PAGE_SIZE)
        results = list(cursor)
        end_time=time.time()

        times.append((end_time-start_time)*1000)
        if not results:
            continue;
        last_val=results[-1][sort_field]
    return times

def run_benchmarks_with_complex_filter_query():
    complex_filter = {"category": "Electronics", "status": "active"}
    print("Benchmarking with complex filter query")
    offset_times=benchmark_offset_pagination_time(complex_filter, "_id")
    cursor_times=benchmark_cursor_pagination_time(complex_filter, "_id")

    plt.subplot(1,2,2)
    plt.plot(offset_times, label="Offset Pagination", color="blue")
    plt.plot(cursor_times, label="Cursor Pagination", color="green")
    plt.title("Pagination Performance (Complex Filter Query) BUT sort by _id")
    plt.ylabel("Time (ms)")
    plt.xlabel("Page Number")
    plt.legend()

def run_benchmarks_with_query_pattern_same_as_physical_disk_layout():
    print("Benchmarking with query pattern same as disk layout that is querying by object _id")
    offset_times=benchmark_offset_pagination_time({}, "_id")
    cursor_times=benchmark_cursor_pagination_time({}, "_id")

    plt.subplot(1,2,1)
    plt.plot(offset_times, label="Offset Pagination", color="blue")
    plt.plot(cursor_times, label="Cursor Pagination", color="green")
    plt.title("Pagination Performance (Query by _id)")
    plt.ylabel("Time (ms)")
    plt.xlabel("Page Number")
    plt.legend()

def main():
    print("Hello from pagination-strategies!")
    seed_data()
    plt.figure(figsize=(14,6))
    run_benchmarks_with_query_pattern_same_as_physical_disk_layout()
    run_benchmarks_with_complex_filter_query()
    plt.show()


if __name__ == "__main__":
    main()
