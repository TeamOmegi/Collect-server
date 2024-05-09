import json
import logging

from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

from crud import ElasticSearchRepository, RedisRepository
from dto.Work import Work
from service import ErrorTraceProcessor
from auth import JwtService

load_dotenv()


class ErrorConsumer:
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

    def activate_listener(self):
        try:
            for message in self.consumer:
                logging.info(f'[ErrorConsumer] activate_listener -> Received message: {message}')
                # 1. 로그 토큰 인증 (project, service id 받기)
                project_id, service_id = JwtService.get_payload_from_token(message.value['token'])
                if service_id is not None and project_id is not None:
                    # 2. 에러 포함 로그인지 확인
                    if message.value['error']:
                        logging.info('[ErrorConsumer] activate_listener -> START: Error message received')
                        work = Work(trace_id='dd',
                                    project_id=project_id,
                                    service_id=service_id,
                                    count=0,
                                    error_trace=message.value
                                    )
                        result = ErrorTraceProcessor.process_work(work)
                        if not result:
                            logging.warning('[ErrorConsumer] activate_listener -> START: Error message received')
                            RedisRepository.enqueue_data(work, os.environ.get("REDIS_FAST_QUE"))
                    else:
                        self.__insert_to_elasticsearch(message.value, project_id, service_id)
        except KeyboardInterrupt:
            print("Aborted by user...", flush=True)
        finally:
            self.consumer.close()

    def __insert_to_elasticsearch(self, data, project_id, service_id):
        logging.info(f'[ErrorConsumer] __insert_to_elasticsearch -> START: {data}')
        data['projectId'] = project_id
        data['serviceId'] = service_id
        ElasticSearchRepository.insert(data)
        logging.info(f'[ErrorConsumer] __insert_to_elasticsearch -> END: {data}')

    def __set_kafka__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=os.getenv('KAFKA_GROUP_ID'),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.consumer.subscribe(self.topics)
