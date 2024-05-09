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

    def __init__(self):
        self.bootstrap_servers = [f'{os.getenv("KAFKA_HOST_1")}:{os.getenv("KAFKA_PORT")}',
                                  f'{os.getenv("KAFKA_HOST_2")}:{os.getenv("KAFKA_PORT")}']
        self.topics = [os.getenv("KAFKA_LOG_TOPIC")]
        self.group_id = os.getenv("KAFKA_GROUP_ID")
        self.__set_kafka__()
        # self.__set_rabbitmq__()

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
                        logging.info(f'Error message received')
                        processed_traces = self.__process_all_traces__(message, project_id, service_id)
                        # 3. MondoDB 저장
                        mongo_result_id = self.__save_to_mongodb__(processed_traces)
                        # 4. MySQL 저장
                        mysql_error_id = self.__save_to_mysql__(processed_traces, mongo_result_id)
                        # 5. RabbitMQ 데이터 전송
                        self.rabbitmq.publish_message(mysql_error_id)
                    # 2.2 에러 로그 아님: project, service id 추가 후 elasticsearch
                    else:
                        logging.info(f'None error message received')
                        ConsumerLogProcessor.insert_to_elasticsearch(message.value)
                        logging.info(f'ElasticSearch save finished')
        except KeyboardInterrupt:
            print("Aborted by user...", flush=True)
            # 재연결 로직이 필요한가?
        finally:
            self.consumer.close()
            # self.rabbitmq.close_connection()

    def __process_all_traces__(self, message, project_id, service_id):
        logging.info(f'Processing trace started')
        processed_traces = ConsumerLogProcessor.process_error(error_trace=message.value, project_id=project_id, service_id=service_id)
        logging.info(f'Processed trace {processed_traces}')
        return processed_traces

    def __save_to_mongodb__(self, processed_traces):
        logging.info(f'Saving to Mongo')
        result = ConsumerLogProcessor.insert_to_mongodb(processed_traces)
        logging.info(f'Saved to Mongo mongo_id: {result}')
        return result

    def __save_to_mysql__(self, error, mongo_id):
        logging.info(f'Saving to MySql')
        error_id = ConsumerLogProcessor.insert_to_mysql(error, mongo_id)
        logging.info(f'Saved to MySql Error {error_id}')
        return error_id

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


