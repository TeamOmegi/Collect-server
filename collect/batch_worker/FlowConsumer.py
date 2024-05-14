import logging
import json
import time
from datetime import datetime, timedelta

from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import os

from auth import JwtService
from dto.RawFlow import RawFlow
from service import FlowTraceProcessor
from crud import ElasticSearchRepository

from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()


class FlowConsumer:
    bootstrap_servers = ''
    topic = []
    group_id = ''
    rabbitmq = None
    consumer = None
    scheduler = None

    def __init__(self):
        self.bootstrap_servers = [f'{os.getenv("KAFKA_HOST_1")}:{os.getenv("KAFKA_PORT")}',
                                  f'{os.getenv("KAFKA_HOST_2")}:{os.getenv("KAFKA_PORT")}']
        self.topic = [os.getenv("KAFKA_LINK_TOPIC")]
        self.group_id = os.getenv("KAFKA_GROUP_ID")
        self.__set_kafka__()
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def __set_kafka__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=os.getenv('KAFKA_GROUP_ID'),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.consumer.subscribe(self.topic)

    def schedule_process_flow(self, trace_id, project_id):
        run_time = datetime.now() + timedelta(seconds=10)
        self.scheduler.add_job(FlowTraceProcessor.process_flow, 'date', run_date=run_time, args=[trace_id, project_id])

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
                        logging.info("Parent span ID starts with '00000'")
                        self.schedule_process_flow(raw_flow.trace_id, raw_flow.project_id)

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
            self.scheduler.shutdown()
