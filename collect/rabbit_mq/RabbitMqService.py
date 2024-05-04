import pika
import os
from dotenv import load_dotenv

load_dotenv()


class RabbitMQSender:
    def __init__(self, host=os.getenv('RABBITMQ_HOST'), queue_name=os.getenv('RABBITMQ_QUEUE')):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()

    def declare_queue(self):
        self.channel.queue_declare(queue=self.queue_name)

    def publish_message(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        print("Sent message: %s" % message)

    def close_connection(self):
        if self.connection:
            self.connection.close()
