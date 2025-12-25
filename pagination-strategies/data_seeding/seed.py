import random
from pymongo import MongoClient, ASCENDING
from faker import Faker

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME="pagingation_test"
COLLECTION_NAME="products"
TOTAL_RECORDS = 100_000

fake = Faker()

def seed_data():
    client = MongoClient(MONGO_URI)
    db=client[DB_NAME]
    collection=db[COLLECTION_NAME]

    collection.drop()

    batch_size = 5000
    batch = []
    for _ in range(TOTAL_RECORDS):
        doc={
            "name": fake.name(),
            "category": random.choice(["Electronics", "Clothing", "Books", "Home", "Toys"]),
            "status": random.choice(["active", "inactive","pending"]),
            "random_score":random.randint(1,1_00_000),
            "created_at": fake.date_time_this_year()
        }
        batch.append(doc)
        if len(batch) >= batch_size:
            collection.insert_many(batch)
            batch = []
    if batch:
        collection.insert_many(batch)
    print("✅ Seeding complete.")