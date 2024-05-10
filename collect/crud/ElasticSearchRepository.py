import json
from pydantic import parse_obj_as

from database.ElasticSearchClient import get_database
from dotenv import load_dotenv
import os

from dto.RawFlow import RawFlow

load_dotenv()

elastic_client = get_database()


def insert(data):
    elastic_client.index(
        index=os.getenv('ELASTICSEARCH_INDEX'),
        body=json.dumps(data)
    )


def insert_with_index(data, index):
    elastic_client.index(
        index=index,
        body=json.dumps(data)
    )


def find_by_trace_id_must_error(trace_id, project_id, service_id):
    result = elastic_client.search(
        index=os.getenv('ELASTICSEARCH_INDEX'),
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "traceId": trace_id
                            }
                        },
                        {
                            "match": {
                                "projectId": project_id
                            }
                        },
                        {
                            "match": {
                                "serviceId": service_id
                            }
                        },
                        {
                            "exists": {
                                "field": "error"
                            }
                        }
                    ]
                }
            }
        }
    )
    if result['hits']['total']['value'] > 0:
        return result['hits']['hits'][0]['_source']
    else:
        return None


def find_parent_span_id(project_id, service_id, span_id):
    result = elastic_client.search(
        index=os.getenv('ELASTICSEARCH_INDEX'),
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "projectId": project_id
                            }
                        },
                        {
                            "match": {
                                "serviceId": service_id
                            }
                        },
                        {
                            "nested": {
                                "path": "spans",
                                "query": {
                                    "bool": {
                                        "must": [
                                            {
                                                "match": {
                                                    "spans.spanId": span_id
                                                }
                                            }
                                        ]
                                    }
                                },
                                "inner_hits": {}
                            }
                        }
                    ]
                }
            }
        }
    )
    if result['hits']['total']['value'] > 0:
        return result['hits']['hits'][0]['_source']
    else:
        return None


def find_all_by_trace_id(trace_id):
    index = os.getenv('ELASTICSEARCH_FLOW_INDEX')
    # Elasticsearch 검색 쿼리
    query = {
        "query": {
            "match": {
                "trace_id": trace_id
            }
        }
    }

    result = elastic_client.search(index=index, body=query)

    # 검색 결과 처리
    traces = []
    for hit in result["hits"]["hits"]:
        trace = hit["_source"]
        raw_flow = RawFlow.parse_obj(trace)
        traces.append(raw_flow)
        traces.append(trace)

    return traces
