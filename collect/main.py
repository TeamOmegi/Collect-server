import logging
logging.basicConfig(level=logging.INFO)

from batch_worker.ErrorConsumer import ErrorConsumer
from batch_worker.RedisWorker import RedisWorker
from dotenv import load_dotenv

load_dotenv()
print("Starting Collect-server", flush=True)


def run_kafka_consumer():
    print("Starting Kafka Consumer", flush=True)
    consumer = ErrorConsumer()
    consumer.activate_listener()


def run_redis_consumer():
    print("Starting Redis Consumer", flush=True)
    redis_worker = RedisWorker()
    redis_worker.run_fast_queue()
    redis_worker.run_slow_queue()


run_kafka_consumer()
run_redis_consumer()