import logging
from datetime import datetime
from typing import List

from crud import ElasticSearchRepository, MongoRepository, MySqlRespository
from database import MySqlClient
from dto.ErrorLog import ErrorLog
from dto.Trace import TraceSpan
from dto.Work import Work
from entity.Error import Error


def process_work(work: Work) -> bool:
    # 1. work에 error_trace가 존재하지 않을 경우 찾아옴
    if work.error_trace is None or len(work.error_trace) == 0:
        trace = __get_error_trace(work)
        if trace is None:
            return False
        work.error_trace = trace
    # 2. ElasticSearch에서 연결된 trace 모두 찾아오기
    traces = __find_all_trace_from_elasticsearch(work)
    if traces is None:
        return False
    # 3. 모아진 trace 가공
    processed_error_log = __process_traces_to_error_log(traces, work)
    if processed_error_log is None:
        return False
    # 4. MongoDB 저장
    mongo_result_id = __insert_to_mongodb(processed_error_log)
    if mongo_result_id is None:
        return False
    # 5. MySql 저장
    mysql_error_id = __insert_to_mysql(processed_error_log, mongo_result_id)
    if mysql_error_id is None:
        return False
    # 6. RabbidMq 전송


def __get_error_trace(data: Work):
    logging.info(f'[ErrorTraceProcessor] __get_error_trace -> START: {data}')
    trace = ElasticSearchRepository.find_by_trace_id_must_error(data.trace_id, data.project_id, data.service_id)
    if trace is None:
        logging.warning(f'[ErrorTraceProcessor] __get_error_trace -> PROBLEM error trace not found: {data}')
        return None
    logging.info(f'[ErrorTraceProcessor] __get_error_trace -> END: {data}')
    return trace


def __find_all_trace_from_elasticsearch(data: Work) -> List | None:
    logging.info(f'[ErrorTraceProcessor] __find_all_trace_from_elasticsearch -> START: {data}')
    traces = []
    current_trace = data.error_trace
    while True:
        traces.insert(0, current_trace)
        parent_trace_id = current_trace['spans'][-1]['parentSpanId']
        if parent_trace_id == '0000000000000000' or parent_trace_id is None:
            break
        found_trace = ElasticSearchRepository.find_parent_span_id(parent_trace_id, data.project_id, data.service_id)
        if found_trace is None:
            logging.warning(f'[ErrorTraceProcessor] __find_all_trace_from_elasticsearch -> PROBLEM trace not found: {data}')
            return None
        current_trace = found_trace
    logging.info(f'[ErrorTraceProcessor] __find_all_trace_from_elasticsearch__ -> END: {traces}')
    return traces


def __process_traces_to_error_log(traces, data: Work) -> ErrorLog | None:
    logging.info(f'[ErrorTraceProcessor] __process_traces_to_error_log -> START: {data}')
    error_type, summary, log = __get_error_metadata__(traces)
    if error_type is None or summary is None or log is None:
        logging.warning(f'[ErrorTraceProcessor] __process_traces_to_error_log -> PROBLEM occurred while processing '
                        f'error: {data}')
        return None
    span_trace = __get_trace_to_span_metadata__(traces)
    if span_trace is None:
        logging.warning(f'[ErrorTraceProcessor] __process_traces_to_error_log -> PROBLEM occurred while processing '
                        f'trace spans: {data}')
        return None
    logging.info(f'[ErrorTraceProcessor] __process_traces_to_error_log -> END: {data}')
    return ErrorLog(
        project_id=data.project_id,
        service_id=data.service_id,
        trace=span_trace,
        error_type=error_type,
        summary=summary,
        time=datetime.now(),
        log=log
    )


def __insert_to_mongodb(data: ErrorLog) -> str:
    logging.info(f'[ErrorTraceProcessor] __insert_to_mongodb -> START: {data}')
    return MongoRepository.insert(data)


def __insert_to_mysql(data: ErrorLog, mongo_id: str) -> object:
    logging.info(f'[ErrorTraceProcessor] __insert_to_mysql -> START: {data}')
    error = Error(
        service_id=data.service_id,
        mongo_id=mongo_id,
        type=data.error_type,
        summary=data.summary,
        time=data.time,
    )

    error_id = MySqlRespository.insert(error, MySqlClient.get_database())
    return error_id


def __get_error_metadata__(traces) -> tuple | None:
    for trace in traces:
        if trace['error']:
            error_type = trace['error']['exception.type']
            summary = "\n".join(f"{key}: {value}" for key, value in list(trace['error']['exception.flow'].items())[:5])
            log = trace['error']['exception.stacktrace']
            return (error_type, summary, log)
    return None


def __get_trace_to_span_metadata__(traces) -> List | None:
    spans = []
    for trace in traces:
        service_name = trace['serviceName']
        for span in trace['spans']:
            spans.append(
                TraceSpan(
                    span_id=span['spanId'],
                    service_name=service_name,
                    name=span['name'],
                    parent_span_id=span['parentSpanId'],
                    kind=span['kind'],
                    attributes=span['attributes'],
                    enter_time=span['spanEnterTime'],
                    exit_time=span['spanExitTime']
                ))
    return spans if spans else None

