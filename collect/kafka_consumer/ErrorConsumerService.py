import json

from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

from rabbit_mq.RabbitMqService import RabbitMQSender
from service import ErrorLogService
from auth import JwtService


load_dotenv()


class ErrorConsumerService:
    bootstrap_servers = ''
    topic = ''
    group_id = ''
    rabbitmq = None
    consumer = None

    def __init__(self,
                 host=os.getenv("KAFKA_HOST"),
                 port=os.getenv("KAFKA_PORT"),
                 topic=os.getenv("KAFKA_LOG_TOPIC"),
                 group_id=os.getenv("KAFKA_GROUP_ID")
                 ):
        self.bootstrap_servers = f'{host}:{port}'
        self.topic = topic
        self.group_id = group_id
        self.__set_kafka__()
        self.__set_rabbitmq__()

    def activate_listener(self):
        try:
            for message in self.consumer:
                print(f"Received message: {message.value}", flush=True)
                # TODO 로그 토큰 인증
                JwtService.decode_token("dd")

                # TODO MongoDB 데이터 저장 (MongoDB, 데이터 형식 수정)
                ErrorLogService.insert_error_log(message.value)

                # TODO 서버 흐름도 데이터 가공, 저장 (MySql)

                # TODO spring boot에 알림 전송
                self.rabbitmq.publish_message(json.dumps(message.value))
        except KeyboardInterrupt:
            print("Aborted by user...", flush=True)
            # 재연결 로직이 필요한가?
        finally:
            self.consumer.close()
            self.rabbitmq.close_connection()

    def __set_rabbitmq__(self):
        self.rabbitmq = RabbitMQSender()
        self.rabbitmq.connect()
        self.rabbitmq.declare_queue()

    def __set_kafka__(self):
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=None, # 추후 변경
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

