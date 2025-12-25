from data_seeding.seed import seed_data
import time
import matplotlib.pyplot as plt
from pymongo import MongoClient

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
        cursor=collection.find(filter_query).sort(sort_field).skip(skip,1).limit(PAGE_SIZE)
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

        if last_val:
            filter_query[sort_field]={"$gt":last_val}
        cursor=collection.find(filter_query).sort(sort_field,1).limit(PAGE_SIZE)
        results = list(cursor)

        end_time=time.time()

        times.append((end_time-start_time)*1000)
        if not results:
            break;
        last_val=results[-1][sort_field]
    return times

def main():
    print("Hello from pagination-strategies!")
    seed_data()


if __name__ == "__main__":
    main()
