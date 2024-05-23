import json
import logging
from datetime import datetime

from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

from crud import ElasticSearchRepository, RedisRepository
from dto.Work import Work
import TestTraceProcessor
from auth import JwtService

load_dotenv()


class ErrorConsumer:
    bootstrap_servers = ''
    topics = []
    group_id = ''
    consumer = None
    count = 0

    def __init__(self):
        self.bootstrap_servers = ['localhost:9092']
        self.topics = [os.getenv("KAFKA_LOG_TOPIC")]
        self.group_id = os.getenv("KAFKA_GROUP_ID")
        self.__set_kafka__()

    def activate_listener(self):
        try:
            for message in self.consumer:
                if self.count == 0:
                    print(f'consumer 시작\n{datetime.now()}')

                try:
                    self.count += 1
                    logging.info(f'[ErrorConsumer] activate_listener -> Received message')
                    logging.debug(f'[ErrorConsumer] activate_listener -> MESSAGE : {message}')

                    # 1. 로그 토큰 인증 (project, service id 받기)
                    project_id, service_id = JwtService.decode_token(message.value['token'])

                    logging.info(f'Project ID: {project_id}')
                    logging.info(f'Service ID: {service_id}')

                    if service_id is not None and project_id is not None:
                        # 3. 에러 포함 로그인지 확인
                        if message.value['error']:
                            logging.info('[ErrorConsumer] activate_listener -> START: Error message received')
                            work = Work(trace_id=message.value['traceId'],
                                        project_id=project_id,
                                        service_id=service_id,
                                        count=0,
                                        error_trace=message.value
                                        )
                            result = TestTraceProcessor.process_work(work)
                            if not result:
                                logging.warning('[ErrorConsumer] activate_listener -> START: Error message received')
                                self.__insert_to_elasticsearch(message.value, project_id, service_id)
                                RedisRepository.enqueue_data(work, os.environ.get("REDIS_FAST_QUE"))
                        else:
                            self.__insert_to_elasticsearch(message.value, project_id, service_id)
                except Exception as e:
                    logging.warning(f'[ErrorConsumer] activate_listener -> ERROR: {e}')
                    # 개별 메시지 처리 중 오류 발생 시 다음 메시지로 넘어감
                    continue

                if self.count % 1000 == 0:
                    print(f'consumer message {self.count}개 소비 완료\n{datetime.now()}')

        except KeyboardInterrupt:
            print("Aborted by user...", flush=True)
        finally:
            self.consumer.close()

    def __insert_to_elasticsearch(self, data, project_id, service_id):
        logging.info(f'[ErrorConsumer] __insert_to_elasticsearch -> START')
        logging.debug(f'[ErrorConsumer] __insert_to_elasticsearch -> START: {data}')
        data['projectId'] = project_id
        data['serviceId'] = service_id
        ElasticSearchRepository.insert(data)
        logging.info(f'[ErrorConsumer] __insert_to_elasticsearch -> END:')
        logging.debug(f'[ErrorConsumer] __insert_to_elasticsearch -> END: {data}')

    def __set_kafka__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=os.getenv('KAFKA_GROUP_ID'),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.consumer.subscribe(self.topics)
