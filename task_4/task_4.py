import threading
import logging
import sys
import os
from time import time
from pymongo import MongoClient, WriteConcern
from pymongo.errors import ConnectionFailure

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(filename='task_4.log', mode='w'),
        logging.StreamHandler(stream=sys.stdout)
    ]
)

# MongoDB Replica Set Configuration
MONGO_URI = "mongodb://mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0&retryWrites=true"
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["test"]


# Reset table to it's initial state
def set_table_initial_state(collection):
    try:
        if collection.find_one({"name": "lidiya"}):
            collection.update_one({"name": "lidiya"}, {"$set": {'likes': 0}})
        else:
            collection.insert_one({"name": "lidiya", "likes": 0})
            logging.info('New record inserted')
        logging.info("Number of likes is set to 0")
    except Exception as e:
        logging.info(f"Error: {e}")


# Check the counter value
def retrieve_table_state(collection):
    try:
        result = collection.find_one({"name": "lidiya"}, {"likes": 1, "_id": 0})
        return result.get("likes")
    except Exception as e:
        logging.info(f"Error: {e}")


# Run function concurrently using 10 threads
def concurrent_run(func, collection, wc):
    set_table_initial_state(collection)
    start_time = time()
    threads = []
    # Run 10 threads
    logging.info(f"Starting concurrent run for {func.__name__} with write concern {wc}")
    for th in range(10):
        thread = threading.Thread(target=func, args={collection})
        threads.append(thread)
        thread.start()
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    logging.info(f"All threads completed. Execution_time = {round(time() - start_time, 2)} s. "
                 f"Final counter: {retrieve_table_state(collection)}")


# Update function
def update_mongo(collection):
    for _ in range(10000):
        collection.find_one_and_update({"name": "lidiya"}, {"$inc": {"likes": 1}})


if __name__ == '__main__':
    concurrent_run(update_mongo, db.get_collection("grades", write_concern=WriteConcern(1)), 1)
    concurrent_run(update_mongo, db.get_collection("grades", write_concern=WriteConcern("majority")), "majority")
