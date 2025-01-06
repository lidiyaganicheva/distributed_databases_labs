import psycopg2
import threading
import logging
import sys
import os
from time import time
from dotenv import load_dotenv

# Set up logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(filename='task_1.log', mode='w'),
        logging.StreamHandler(stream=sys.stdout)
    ]
)



# Connect to database
def connect_to_db():
    load_dotenv()
    try:
        conn = psycopg2.connect(f"dbname={os.getenv('POSTGRES_DB_NAME')} "
                                f"user={os.getenv('POSTGRES_USER')} "
                                f"password={os.getenv('POSTGRES_PASSWORD')} "
                                f"host={os.getenv('POSTGRES_HOST')} "
                                f"port={os.getenv('POSTGRES_PORT')}")
        cursor = conn.cursor()
        # logging.info("Connection successful")
    except psycopg2.Error as e:
        logging.info(f"Error connecting to the database: {e}")
    return conn, cursor


# Set up global connection
conn, cursor = connect_to_db()


# Reset table to it's initial state
def set_table_initial_state():
    global conn, cursor
    cursor.execute('''UPDATE user_counter SET counter = 1, version = 0 WHERE user_id = 1''')
    conn.commit()


# Check the counter value
def retrieve_table_state():
    global conn, cursor
    cursor.execute('''SELECT counter FROM user_counter WHERE user_id = 1''')
    counter = cursor.fetchone()[0]
    return counter


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


# Lost update
def lost_update():
    global conn
    cursor = conn.cursor()
    for i in range(10000):
        cursor.execute('''SELECT counter FROM user_counter WHERE user_id = 1''')
        counter = cursor.fetchone()[0]
        counter = counter + 1
        cursor.execute(f'''update user_counter set counter = {counter} where user_id = {1}''')
        conn.commit()
    cursor.close()


# In place update
def in_place_update():
    global conn, cursor
    for i in range (10000):
        cursor.execute('''UPDATE user_counter SET counter = counter + 1, 
            version = 0 WHERE user_id = 1''')
        conn.commit()


# Row level locking
def row_level_locking():
    conn, cursor = connect_to_db()
    for i in range(10000):
        cursor.execute('''SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE''')
        counter = cursor.fetchone()[0]
        counter = counter + 1
        cursor.execute(f'''update user_counter set counter = {counter} where user_id = {1}''')
        conn.commit()
    conn.close()


# Optimistic concurrency control
def optimistic_concurrency_control():
    global conn
    cursor = conn.cursor()
    for i in range(10000):
        count = 0
        while count <= 0:
            cursor.execute('''SELECT counter, version FROM user_counter WHERE user_id = 1 FOR UPDATE''')
            row = cursor.fetchone()
            counter = row[0]
            version = row[1]
            counter = counter + 1
            cursor.execute(f'''update user_counter set counter = {counter}, version = {version + 1} 
                where user_id = {1} and version = {version}''')
            conn.commit()
            count = cursor.rowcount
    cursor.close()


if __name__ == '__main__':
    concurrent_run(lost_update)
    concurrent_run(in_place_update)
    concurrent_run(row_level_locking)
    concurrent_run(optimistic_concurrency_control)
    conn.close()



