import json
import logging

import pika
import os
from dotenv import load_dotenv

from dto.Flow import Flow

load_dotenv()


class RabbitMQFlowSender:
    def __init__(self, host=os.getenv('RABBITMQ_HOST'), port=os.getenv('RABBITMQ_PORT'),
                 queue_name=os.getenv('RABBITMQ_FLOW_QUEUE')):
        logging.warning(f'RabbitMQSender: {os.getenv("RABBITMQ_HOST")}, {os.getenv("RABBITMQ_FLOW_QUEUE")}')
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        credentials = pika.PlainCredentials(username=os.getenv('RABBITMQ_USER'), password=os.getenv('RABBITMQ_PASS'))
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials))
        self.channel = self.connection.channel()

    def declare_queue(self):
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def publish_flow_message(self, data: Flow):
        self.connect()

        try:
            body = {
                "project_id": data.project_id,
                "service_flow": data.service_flow_asc
            }

            json_body = json.dumps(body)

            # 메시지 publish 시 delivery_mode를 2로 설정하여 메시지 영속화
            publish = self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 메시지 영속화
                )
            )
            logging.info(json_body)
            logging.warning(publish)
        except Exception as e:
            logging.error(f'Failed to publish message: {str(e)}')

    def close_connection(self):
        if self.connection:
            self.connection.close()
