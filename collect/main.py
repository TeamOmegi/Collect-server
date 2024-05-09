import logging

from dotenv import load_dotenv
from kafka_consumer.ErrorConsumerService import ErrorConsumerService

load_dotenv()
print("Starting Collect-server", flush=True)
logging.basicConfig(level=logging.INFO)


def run():
    print("Starting Kafka Consumer", flush=True)
    consumer = ErrorConsumerService()
    consumer.activate_listener()


run()
