import logging
import time

# logging.basicConfig(level=logging.INFO)

from batch_worker.FlowConsumer import FlowConsumer
import threading
# from batch_worker.ErrorConsumer import ErrorConsumer
from test.TestRedisWorker import RedisWorker
from dotenv import load_dotenv
from test import TestProducer
from test.TestConsumer import ErrorConsumer

load_dotenv()

consumers = [ErrorConsumer() for _ in range(10)]


def start_consumer(consumer_instance):
    consumer_instance.activate_listener()


def main():
    print("Starting Collect-server", flush=True)

    flow_consumer = FlowConsumer()
    redis_worker = RedisWorker()

    consumer_threads = [threading.Thread(target=start_consumer, args=(consumer,)) for consumer in consumers]
    flow_consumer_thread = threading.Thread(target=flow_consumer.activate_listener)
    redis_worker_fast_thread = threading.Thread(target=redis_worker.run_fast_queue)
    redis_worker_slow_thread = threading.Thread(target=redis_worker.run_slow_queue)

    # Start all consumer threads
    for thread in consumer_threads:
        thread.start()
    flow_consumer_thread.start()
    redis_worker_fast_thread.start()
    redis_worker_slow_thread.start()

    try:
        while True:
            # Sleep for 3 seconds
            time.sleep(3)

            # Calculate total consumed messages
            total_consumed = sum(consumer.get_consumed_count() for consumer in consumers)

            # Print the total consumed messages
            print(f"Total messages consumed: {total_consumed}", flush=True)

    except KeyboardInterrupt:
        print("Stopping Collect-server")

    # Join all consumer threads
    for thread in consumer_threads:
        thread.join()
    flow_consumer_thread.join()
    redis_worker_fast_thread.join()
    redis_worker_slow_thread.join()


main()
