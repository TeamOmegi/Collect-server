import json

from database import MySqlClient
from dto.RawFlow import RawFlow
from typing import List
import logging

from entity.ServiceLink import ServiceLink
from rabbit_mq.RabbitMqFlowService import RabbitMQFlowSender
from crud import ElasticSearchRepository, MySqlRespository
from dto.Flow import Flow

rabbitmq = RabbitMQFlowSender()
rabbitmq.connect()
rabbitmq.declare_queue()


def process_flow(raw_flow: RawFlow) -> bool:
    services = __find_all_trace_from_elasticsearch(raw_flow)
    processed_flow = __process_traces_to_flow_data(services, raw_flow)
    __insert_to_mysql(processed_flow)
    __send_to_rabbitmq(processed_flow.project_id)
    return True


def __find_all_trace_from_elasticsearch(data: RawFlow) -> List | None:
    logging.info(f'[FlowTraceProcessor] __find_all_trace_from_elasticsearch -> START: {data}')
    services = []
    trace_id = data.trace_id

    found_trace = ElasticSearchRepository.find_all_by_trace_id(trace_id)
    services.append(data)

    if found_trace is not None:
        sorted_found_trace = sorted(found_trace, key=lambda x: x.span_enter_time)
        services.extend(sorted_found_trace)

    logging.info(f'[FlowTraceProcessor] __find_all_trace_from_elasticsearch -> END: {services}')
    return services


def __process_traces_to_flow_data(traces, data: RawFlow) -> Flow | None:
    logging.info(f'[FlowTraceProcessor] __process_traces_to_flow_data -> START')
    services = []

    for trace in traces:
        body = {
            "serviceId": trace.service_id,
            "spanEnterTime": str(trace.span_enter_time)
        }

        services.append(body)

    return Flow(
        trace_id=data.trace_id,
        project_id=data.project_id,
        service_flow_asc=services
    )


def __insert_to_mysql(data: Flow):
    logging.info(f'[FlowTraceProcessor] __insert_to_mysql -> START')
    logging.debug(f'[FlowTraceProcessor] __insert_to_mysql -> DATA: {data}')

    services = data.service_flow_asc
    pre_service = -1

    for service in services:
        if pre_service == -1:
            pre_service = service.service_id
            continue

        service_link = ServiceLink(
            service_id=pre_service,
            linked_service_id=service.service_id,
            enabled=True
        )

        pre_service=service.service_id

        insert = MySqlRespository.insertServiceLink(service_link, MySqlClient.get_database())
        logging.info(f'[FlowTraceProcessor] __insert_to_mysql -> INSERT_ID: {insert}')


def __send_to_rabbitmq(project_id):
    logging.info(f'[FlowTraceProcessor] __send_to_rabbitmq -> START: {project_id}')
    result = rabbitmq.publish_flow_message(project_id)
    logging.info(f'[FlowTraceProcessor] __send_to_rabbitmq -> END: {result}')
