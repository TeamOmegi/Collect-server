from dto.RawFlow import RawFlow

def process_flow(flow: RawFlow) -> bool:
    # ElasticSearch에서 연결된 trace 모두 찾아오기
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
    __send_to_rabbitmq(mysql_error_id)
    return True

def find_all_trace_from_elasticsearch(data: Work) -> List | None:
    logging.info(f'[ErrorTraceProcessor] __find_all_trace_from_elasticsearch -> START: {data}')
    traces = []
    current_trace = data.error_trace
    while True:
        traces.insert(0, current_trace)
        parent_trace_id = current_trace['spans'][-1]['parentSpanId']
        if parent_trace_id == '0000000000000000' or parent_trace_id is None:
            break
        found_trace = ElasticSearchRepository.find_parent_span_id(data.project_id, data.service_id, parent_trace_id)
        if found_trace is None:
            logging.warning(f'[ErrorTraceProcessor] __find_all_trace_from_elasticsearch -> PROBLEM trace not found: {data}')
            return None
        current_trace = found_trace
    logging.info(f'[ErrorTraceProcessor] __find_all_trace_from_elasticsearch__ -> END: {traces}')
    return traces