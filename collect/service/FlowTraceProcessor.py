import json

from crud.MySqlRespository import check_service_link_exists
from database import MySqlClient
from dto.RawFlow import RawFlow
from typing import List
import logging

from entity.ServiceLink import ServiceLink
from crud import ElasticSearchRepository, MySqlRespository
from dto.Flow import Flow


def process_flow(trace_id, project_id) -> bool:
    services = __find_all_trace_from_elasticsearch(trace_id)
    logging.info(f'[FlowTraceProcessor] __find_all_trace_from_elasticsearch -> services: {services}')
    processed_flow = __process_traces_to_flow_data(services, trace_id, project_id)
    __insert_to_mysql_if_not_exist(processed_flow)
    return True


def __find_all_trace_from_elasticsearch(trace_id) -> List | None:
    logging.info(f'[FlowTraceProcessor] __find_all_trace_from_elasticsearch -> START: {trace_id}')
    services = []

    found_trace = ElasticSearchRepository.find_all_by_trace_id(trace_id)
    logging.info(f'[FlowTraceProcessor] __find_all_trace_from_elasticsearch -> found_trace: {found_trace}')

    if found_trace is not None:
        sorted_found_trace = sorted(found_trace, key=lambda x: x.span_enter_time)
        services.extend(sorted_found_trace)

    logging.info(f'[FlowTraceProcessor] __find_all_trace_from_elasticsearch -> END: {services}')
    return services


def __process_traces_to_flow_data(traces, trace_id, project_id) -> Flow | None:
    logging.info(f'[FlowTraceProcessor] __process_traces_to_flow_data -> START')
    services = []

    for trace in traces:
        body = {
            "serviceId": trace.service_id,
            "spanEnterTime": str(trace.span_enter_time)
        }

        services.append(body)

    return Flow(
        trace_id=trace_id,
        project_id=project_id,
        service_flow_asc=services
    )


def __insert_to_mysql_if_not_exist(data: Flow):
    logging.info('[FlowTraceProcessor] __insert_to_mysql -> START')
    logging.info(f'[FlowTraceProcessor] __insert_to_mysql -> DATA: {data}')

    services = data.service_flow_asc
    pre_service = None
    session = MySqlClient.get_database()

    try:
        for service in services:
            if pre_service is None:
                pre_service = service['serviceId']
                continue

            service_link = ServiceLink(
                service_id=pre_service,
                linked_service_id=service['serviceId'],
                enabled=True
            )

            exists = check_service_link_exists(pre_service, service_link.linked_service_id, session)
            logging.info(f'[FlowTraceProcessor] __insert_to_mysql -> exists: {exists}')

            if not exists:
                insert = MySqlRespository.insert_service_link(service_link, session)
                logging.info(f'[FlowTraceProcessor] __insert_to_mysql -> INSERT_ID: {insert}')

            pre_service = service['serviceId']
    except Exception as e:
        logging.error(f'[FlowTraceProcessor]')
    finally:
        session.close()
