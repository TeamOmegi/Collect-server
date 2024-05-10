from dto.RawFlow import RawFlow
from typing import List
import logging
from rabbit_mq.RabbitMqFlowService import RabbitMQFlowSender
from crud import ElasticSearchRepository
from dto.Flow import Flow

rabbitmq = RabbitMQFlowSender()
rabbitmq.connect()
rabbitmq.declare_queue()


def process_flow(raw_flow: RawFlow) -> bool:
    services = __find_all_trace_from_elasticsearch(raw_flow)
    processed_flow = __process_traces_to_flow_data(services, raw_flow)
    __send_to_rabbitmq(processed_flow)
    return True


def __find_all_trace_from_elasticsearch(data: RawFlow) -> List | None:
    logging.info(f'[FlowTraceProcessor] __find_all_trace_from_elasticsearch -> START: {data}')
    services = []
    trace_id = data.trace_id

    found_trace = ElasticSearchRepository.find_all_by_trace_id(trace_id)
    services.insert(0, data)

    if found_trace is not None:
        services.append(found_trace)

    logging.info(f'[FlowTraceProcessor] __find_all_trace_from_elasticsearch -> END: {services}')
    return services


def __process_traces_to_flow_data(traces, data: RawFlow) -> Flow | None:
    logging.info(f'[FlowTraceProcessor] __process_traces_to_flow_data -> START')
    service_names = []

    for trace in traces:
        service_names.append(trace['service_name'])

    return Flow(
        trace_id=data.trace_id,
        project_id=data.project_id,
        service_flow_asc=service_names
    )


def __send_to_rabbitmq(data: Flow):
    logging.info(f'[FlowTraceProcessor] __send_to_rabbitmq -> START: {data}')
    result = rabbitmq.publish_flow_message(data)
    logging.info(f'[FlowTraceProcessor] __send_to_rabbitmq -> END: {result}')
