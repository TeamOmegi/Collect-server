import json
import logging

from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

from rabbit_mq.RabbitMqService import RabbitMQSender
from service import ConsumerLogProcessor
from auth import JwtService


load_dotenv()


class ErrorConsumerService:
    bootstrap_servers = ''
    topics = []
    group_id = ''
    rabbitmq = None
    consumer = None

    def __init__(self,
                 host=os.getenv("KAFKA_HOST"),
                 port=os.getenv("KAFKA_PORT"),
                 log_topic=os.getenv("KAFKA_LOG_TOPIC"),
                 link_topic=os.getenv("KAFKA_LINK_TOPIC"),
                 group_id=os.getenv("KAFKA_GROUP_ID")
                 ):
        self.bootstrap_servers = f'{host}:{port}'
        self.topics = [log_topic, link_topic]
        self.group_id = group_id
        self.__set_kafka__()
        # self.__set_rabbitmq__()
        self.logger = logging.getLogger('ErrorConsumerService')

    def activate_listener(self):
        try:
            for message in self.consumer:
                logging.info(f'Received message: {message}')
                # 1. 로그 토큰 인증 (project, service id 받기)
                project_id, service_id  = JwtService.get_payload_from_token(message.value['token'])
                if service_id is not None and project_id is not None:
                    # 2. 에러 포함 로그인지 확인
                    # 2.1 에러 로그: 지금까지 모인 trace 조회, 가공
                    if message.value['error']:
                        logging.WARN(f'Error message received')
                        processed_traces = ConsumerLogProcessor.process_error(message.value, project_id,service_id)
                        logging.WARN(f'Processed trace {processed_traces}')
                        # 3. MondoDB 저장
                        ConsumerLogProcessor.insert_to_mongodb(processed_traces)
                        logging.WARN(f'Saved to Mongo')
                        # 4. MySQL 저장
                        # error_id = ConsumerLogProcessor.insert_to_mysql(processed_traces)
                        # 5. RabbitMQ 데이터 전송
                        # self.rabbitmq.publish_message(error_id)
                    # 2.2 에러 로그 아님: project, service id 추가 후 elasticsearch
                    else:
                        logging.WARN(f'None error message received')
                        ConsumerLogProcessor.insert_to_elasticsearch(message.value)
                        logging.WARN(f'ElasticSearch save finished')
        except KeyboardInterrupt:
            print("Aborted by user...", flush=True)
            # 재연결 로직이 필요한가?
        finally:
            self.consumer.close()
            # self.rabbitmq.close_connection()

    def __set_rabbitmq__(self):
        self.rabbitmq = RabbitMQSender()
        self.rabbitmq.connect()
        self.rabbitmq.declare_queue()

    def __set_kafka__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=None, # 추후 변경
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.consumer.subscribe(self.topics)


