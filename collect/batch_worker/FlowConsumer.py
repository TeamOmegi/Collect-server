import logging
import json
import time

from kafka import KafkaConsumer, KafkaProducer
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
    producer = None

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

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def activate_listener(self):
        try:
            for message in self.consumer:
                second_send = message.value.get('secondSend', 0)
                logging.info(f'[FlowConsumer] activate_listener -> Second send: {second_send}')
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

                    if 0 < second_send < 10:
                        logging.info(f'Second send : {second_send}')
                        traces = ElasticSearchRepository.find_all_by_trace_id(raw_flow.trace_id)

                        if not traces:
                            message.value['secondSend'] = second_send + 1
                            self.producer.send(os.getenv('KAFKA_LINK_TOPIC'), value=message.value)
                        else:
                            FlowTraceProcessor.process_flow(raw_flow)

                    elif raw_flow.parent_span_id.startswith('00000') or raw_flow.parent_span_id is None:
                        logging.info("Parent span ID starts with '00000'")
                        message.value['secondSend'] = 1
                        self.producer.send(os.getenv('KAFKA_LINK_TOPIC'), value=message.value)

                    else:
                        logging.info("Normal span")
                        index = os.getenv('ELASTICSEARCH_FLOW_INDEX')
                        raw_flow_dict = raw_flow.dict()
                        ElasticSearchRepository.insert_with_index(raw_flow_dict, index)

        except KeyboardInterrupt:
            logging.info("Aborted by user...")
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
        finally:
            self.consumer.close()
            self.producer.close()
