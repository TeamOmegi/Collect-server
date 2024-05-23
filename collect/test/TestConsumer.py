import json
import logging
import threading
from datetime import datetime

from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

from crud import ElasticSearchRepository, RedisRepository
from dto.Work import Work
from test.TestTraceProcessor import process_work
from auth import JwtService

load_dotenv()


class ErrorConsumer:
    bootstrap_servers = ''
    topics = []
    group_id = ''
    consumer = None
    consumed_count = 0

    def __init__(self):
        logging.info("들어왔습니당")
        self.bootstrap_servers = [f'{os.getenv("KAFKA_HOST_1")}:{os.getenv("KAFKA_PORT")}',
                                  f'{os.getenv("KAFKA_HOST_2")}:{os.getenv("KAFKA_PORT")}']
        self.topics = [os.getenv("KAFKA_LOG_TOPIC")]
        self.group_id = os.getenv("KAFKA_GROUP_ID")
        self.__set_kafka__()
        self.lock = threading.Lock()

    def activate_listener(self):
        try:
            for message in self.consumer:
                if self.consumed_count == 0:
                    print(f'consumer 시작\n{datetime.now()}', flush=True)

                try:
                    with self.lock:
                        self.consumed_count += 1
                    # 1. 로그 토큰 인증 (project, service id 받기)
                    project_id, service_id = JwtService.decode_token(message.value['token'])

                    if service_id is not None and project_id is not None:
                        # 3. 에러 포함 로그인지 확인
                        if message.value['error']:
                            work = Work(trace_id=message.value['traceId'],
                                        project_id=project_id,
                                        service_id=service_id,
                                        count=0,
                                        error_trace=message.value
                                        )
                            result = process_work(work)
                            if not result:
                                self.__insert_to_elasticsearch(message.value, project_id, service_id)
                                RedisRepository.enqueue_data(work, os.environ.get("REDIS_FAST_QUE"))
                        else:
                            self.__insert_to_elasticsearch(message.value, project_id, service_id)
                except Exception as e:
                    # 개별 메시지 처리 중 오류 발생 시 다음 메시지로 넘어감
                    continue

        except KeyboardInterrupt:
            print("Aborted by user...", flush=True)
        finally:
            self.consumer.close()

    def __insert_to_elasticsearch(self, data, project_id, service_id):
        data['projectId'] = project_id
        data['serviceId'] = service_id
        ElasticSearchRepository.insert(data)

    def __set_kafka__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=os.getenv('KAFKA_GROUP_ID'),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.consumer.subscribe(self.topics)

    def get_consumed_count(self):
        with self.lock:
            return self.consumed_count
