import json

from database.ElasticSearchClient import get_database
from dotenv import load_dotenv
import os


load_dotenv()

elastic_client = get_database()


def insert(data):
    elastic_client.index(
        index=os.getenv('ELASTICSEARCH_INDEX'),
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
        return result['hits']['hits'][0]['inner_hits']['spans']['hits']['hits'][0]['_source']
    else:
        return None

# print(find_by_trace_id_must_error(project_id=8, service_id=2, trace_id='48e46b7c5e9b74e5657db623ead9bcdf')['spans'][-1]['parentSpanId'])