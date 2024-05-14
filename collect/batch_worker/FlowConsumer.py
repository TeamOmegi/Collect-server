import logging
import json
import time

from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

from auth import JwtService
from dto.RawFlow import RawFlow
from service import FlowTraceProcessor
from crud import ElasticSearchRepository

load_dotenv()


class FlowConsumer:
    bootstrap_servers = ''
    topic = []
    group_id = ''
    rabbitmq = None
    consumer = None

    def __init__(self):
        self.bootstrap_servers = [f'{os.getenv("KAFKA_HOST_1")}:{os.getenv("KAFKA_PORT")}',
                                  f'{os.getenv("KAFKA_HOST_2")}:{os.getenv("KAFKA_PORT")}']
        self.topic = [os.getenv("KAFKA_LINK_TOPIC")]
        self.group_id = os.getenv("KAFKA_GROUP_ID")
        self.__set_kafka__()

    def __set_kafka__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=os.getenv('KAFKA_GROUP_ID'),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.consumer.subscribe(self.topic)

    def activate_listener(self):
        try:
            for message in self.consumer:
                logging.info(f'[FlowConsumer] activate_listener -> Received message: {message}')
                project_id, service_id = JwtService.decode_token(message.value['token'])
                logging.info(f'Project ID: {project_id}')
                logging.info(f'Service ID: {service_id}')

                if service_id is not None and project_id is not None:
                    raw_flow = RawFlow(trace_id=message.value['traceId'],
                                       project_id=project_id,
                                       service_id=service_id,
                                       span_id=message.value['spanId'],
                                       parent_span_id=message.value['parentSpanId'],
                                       span_enter_time=message.value['spanEnterTime'],
                                       span_exit_time=message.value['spanExitTime']
                                       )

                    if raw_flow.parent_span_id.startswith('00000') or raw_flow.parent_span_id is None:
                        time.sleep(1)
                        FlowTraceProcessor.process_flow(raw_flow)
                    else:
                        index = os.getenv('ELASTICSEARCH_FLOW_INDEX')
                        raw_flow_dict = raw_flow.dict()
                        ElasticSearchRepository.insert_with_index(raw_flow_dict, index)
        except KeyboardInterrupt:
            print("Aborted by user...", flush=True)
        finally:
            self.consumer.close()
