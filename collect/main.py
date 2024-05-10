import logging
import threading
from batch_worker.ErrorConsumer import ErrorConsumer
from batch_worker.RedisWorker import RedisWorker
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()


def main():
    print("Starting Collect-server", flush=True)
    consumer = ErrorConsumer()
    redis_worker = RedisWorker()

    consumer_thread = threading.Thread(target=consumer.activate_listener)
    redis_worker_fast_thread = threading.Thread(target=redis_worker.run_fast_queue)
    redis_worker_slow_thread = threading.Thread(target=redis_worker.run_slow_queue)

    consumer_thread.start()
    redis_worker_fast_thread.start()
    redis_worker_slow_thread.start()


main()
