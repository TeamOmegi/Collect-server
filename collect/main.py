import logging

logging.basicConfig(level=logging.DEBUG)

from batch_worker.FlowConsumer import FlowConsumer
import threading
from batch_worker.ErrorConsumer import ErrorConsumer
from batch_worker.RedisWorker import RedisWorker
from dotenv import load_dotenv

load_dotenv()


def main():
    print("Starting Collect-server", flush=True)
    flow_consumer = FlowConsumer()
    consumer = ErrorConsumer()
    redis_worker = RedisWorker()

    consumer_thread = threading.Thread(target=consumer.activate_listener)
    flow_consumer_thread = threading.Thread(target=flow_consumer.activate_listener)
    redis_worker_fast_thread = threading.Thread(target=redis_worker.run_fast_queue)
    redis_worker_slow_thread = threading.Thread(target=redis_worker.run_slow_queue)

    consumer_thread.start()
    flow_consumer_thread.start()
    redis_worker_fast_thread.start()
    redis_worker_slow_thread.start()

    consumer_thread.join()
    flow_consumer_thread.join()
    redis_worker_fast_thread.join()
    redis_worker_slow_thread.join()


main()
