import json
import logging

import pika
import os
from dotenv import load_dotenv

load_dotenv()


class RabbitMQSender:
    def __init__(self, host=os.getenv('RABBITMQ_HOST'), port=os.getenv('RABBITMQ_PORT'), queue_name=os.getenv('RABBITMQ_QUEUE')):
        logging.warning(f'RabbitMQSender: {os.getenv("RABBITMQ_HOST")}, {os.getenv("RABBITMQ_QUEUE")}')
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        credentials = pika.PlainCredentials(username=os.getenv('RABBITMQ_USER'), password=os.getenv('RABBITMQ_PASS'))
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials))
        self.channel = self.connection.channel()

    def declare_queue(self):
        self.channel.queue_declare(queue=self.queue_name)

    def publish_message(self, error_id):

        body = {
            "error_id": error_id,
        }

        json_body = json.dumps(body)

        publish = self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=json_body)
        logging.info(json_body)
        logging.warning(publish)


    def close_connection(self):
        if self.connection:
            self.connection.close()
