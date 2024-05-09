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


def find_parent_span_id(project_id, service_id, span_id: str):
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
                                    "match": {
                                        "spans.spanId": span_id
                                    }
                                },
                                "inner_hits": {
                                    "_source": True
                                }
                            }
                        }
                    ]
                }
            }
        }
    )
    if result['hits']['total']['value'] > 0:
        return result['hits']['hits'][0]['_source'][0]
    else:
        return None