import json
from datetime import datetime
from typing import List

from dto.ErrorLog import ErrorLog
from crud import ElasticSearchRepository as ElasticSearchRepository, MongoRepository
from dto.Trace import TraceSpan


def insert_to_elasticsearch(data, project_id, service_id):
    data['projectId'] = project_id
    data['serviceId'] = service_id
    ElasticSearchRepository.insert(data)


def insert_to_mongodb(data: ErrorLog):
    MongoRepository.insert(data)


def process_error(error_trace, project_id, service_id) -> ErrorLog:
    traces = _find_all_trace_from_elasticsearch(error_trace, project_id, service_id)
    if traces is None:
        # To Redis
        return None
    return _process_traces(traces)


# 1. ElasticSearch에서 연결된 trace 모두 찾아오기
def _find_all_trace_from_elasticsearch(error_trace, project_id, service_id) -> List:
    traces = []
    current_trace = error_trace
    while True:
        traces.insert(0,current_trace)
        parent_trace_id = current_trace['spans'][-1]['parentSpanId']
        if parent_trace_id == 0000000000000000 or parent_trace_id is None:
            break
        found_trace = ElasticSearchRepository.find_parent_span_id(parent_trace_id, project_id, service_id)
        if found_trace is None:
            return None
        current_trace = found_trace
    return traces


# 2. 모아진 trace 가공
def _process_traces(traces, project_id, service_id) -> ErrorLog:
    processed_error = _process_error(traces)
    if processed_error:
        error_type, summary, log = processed_error[0]
    else:
        error_type, summary, log = None, None, None
    return ErrorLog(
        project_id=project_id,
        service_id=service_id,
        trace=_process_spans(traces),
        error_type=error_type,
        summary=summary,
        log=log
    )


def _process_spans(traces):
    spans = []
    for trace in traces:
        service_name = trace['service-name']
        for span in trace['spans']:
            enter_time = datetime.strptime(span['span enter-time'], "%Y-%m-%d %H:%M:%S.%f")
            exit_time = datetime.strptime(span['span exit-time'], "%Y-%m-%d %H:%M:%S.%f")
            spans.append(
                TraceSpan(
                    span_id=span['spanId'],
                    service_name=service_name,
                    name=span['name'],
                    parent_span_id=span['parentSpanId'],
                    kind=span['kind'],
                    arguments=span['attributes']['arguments'],
                    enter_time=enter_time,
                    exit_time=exit_time
                ))
    return spans


def _process_error(traces):
    processed_data = []
    for trace in traces:
        if trace['error']:
            error_type = trace['error']['exception.type']
            summary = "\n".join(f"{key}: {value}" for key, value in list(trace['error']['exception.flow'].items())[:10])
            log = trace['error']['exception.stacktrace']
            processed_data.append((error_type, summary, log))
            break
    return processed_data

