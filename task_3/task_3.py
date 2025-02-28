import threading
import logging
import sys
import os
from neo4j import GraphDatabase
from time import time
from dotenv import load_dotenv

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(filename='task_3.log', mode='w'),
        logging.StreamHandler(stream=sys.stdout)
    ]
)

load_dotenv()

# Define connection credentials
URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

# Create Neo4j driver
driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))


# Reset table to it's initial state
def set_table_initial_state():
    global driver
    try:
        with driver.session() as session:
            session.run("""MATCH (i:Item {name: "LG43"}) SET i.likes = 0;""")
            logging.info("Number of likes is set to 0")
    except Exception as e:
        logging.info(f"Error: {e}")


# Check the counter value
def retrieve_table_state():
    global driver
    try:
        with driver.session() as session:
            result = session.run("""MATCH (i:Item {name: "LG43"}) RETURN i.likes""")
            return [str(r).split("=")[1] for r in result][0]
    except Exception as e:
        logging.info(f"Error: {e}")


# Run function concurrently using 10 threads
def concurrent_run(func):
    set_table_initial_state()
    start_time = time()
    threads = []
    # Run 10 threads
    logging.info(f"Starting concurrent run for {func.__name__}")
    for th in range(10):
        thread = threading.Thread(target=func)
        threads.append(thread)
        thread.start()
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    logging.info(f"All threads completed. Execution_time = {round(time() - start_time, 2)} s. "
                 f"Final counter: {retrieve_table_state()}")


# Update function
def update():
    global driver
    session = driver.session()
    for i in range(10000):
        session.execute_write(create_tx)


# Transaction function
def create_tx(tx):
    query = """MATCH (i:Item {name: "LG43"}) SET i.likes = i.likes+1;"""
    result = tx.run(query)
    record = result.single()
    return record


if __name__ == '__main__':
    concurrent_run(update)
    driver.close()
