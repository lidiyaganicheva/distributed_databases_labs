import threading
import logging
import sys
from time import time, sleep
from hazelcast import HazelcastClient


# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(filename='task_2.log', mode='w'),
        logging.StreamHandler(stream=sys.stdout)
    ]
)


client = HazelcastClient(
    cluster_name="dev",
    cluster_members=[
        "192.168.1.147:5701",
        "192.168.1.147:5702",
        "192.168.1.147:5703"
    ]
)


class Value:
    def __init__(self, other=0):
        self.amount = other

    def __eq__(self, other):
        if not isinstance(other, Value):
            return False
        return self.amount == other.amount


# Reset all data structures to their initial state
def clear_all_states():
    map_1 = client.get_map("map-without-blocking").blocking()
    map_1.clear()
    map_1.put_if_absent("key", 0)
    map_2 = client.get_map("map-with-pessimistic-blocking").blocking()
    map_2.clear()
    map_2.put_if_absent("key", 0)
    map_3 = client.get_map("map-with-optimistic-blocking").blocking()
    map_3.clear()
    map_3.put_if_absent("key", Value(0))
    al = client.cp_subsystem.get_atomic_long("atomic-long").blocking()
    al.set(0)
    logging.info('All counters are reset to initial state')


# Check for final value
def get_final_value(func_name: str):
    key = "key"
    if func_name == 'counter_without_blocking':
        map_1 = client.get_map("map-without-blocking").blocking()
        return map_1.get(key)
    if func_name == 'counter_with_pessimistic_blocking':
        map_2 = client.get_map("map-with-pessimistic-blocking").blocking()
        return map_2.get(key)
    if func_name == 'counter_with_optimistic_blocking':
        map_3 = client.get_map("map-with-optimistic-blocking").blocking()
        return map_3.get("key").amount
    if func_name == 'counter_iatomiclong':
        al = client.cp_subsystem.get_atomic_long("atomic-long").blocking()
        return al.get()


# Run function concurrently using 10 threads.
def concurrent_run(func):
    global client
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
                 f"Final counter: {get_final_value(func.__name__)}")


# Implement counter without blocking
def counter_without_blocking():
    global client
    task_map = client.get_map("map-without-blocking").blocking()
    key = "key"
    for i in range(10000):
        value = task_map.get(key)
        sleep(0.01)
        task_map.put("key", value + 1)


# Implement counter with pessimistic blocking
def counter_with_pessimistic_blocking():
    global client
    task_map = client.get_map("map-with-pessimistic-blocking").blocking()
    key = "key"
    for i in range(10000):
        task_map.lock(key)
        try:
            value = task_map.get(key)
            sleep(0.01)
            task_map.put(key, value + 1)
        finally:
            task_map.unlock(key)


# Implement counter with optimistic blocking
def counter_with_optimistic_blocking():
    global client
    task_map = client.get_map("map-with-optimistic-blocking").blocking()
    key = "key"
    for i in range(10000):
        while True:
            old_value = task_map.get(key)
            new_value = Value(old_value.amount + 1)
            sleep(0.01)
            if task_map.replace_if_same(key, old_value, new_value):
                break


def counter_iatomiclong():
    global client
    task_ds = client.cp_subsystem.get_atomic_long("atomic-long").blocking()
    for i in range(10000):
        task_ds.get_and_increment()


if __name__ == '__main__':
    clear_all_states()
    concurrent_run(counter_without_blocking)
    concurrent_run(counter_with_pessimistic_blocking)
    concurrent_run(counter_with_optimistic_blocking)
    concurrent_run(counter_iatomiclong)
    client.shutdown()
