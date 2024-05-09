import logging

from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

load_dotenv()


def get_database():
    elasticsearch_url = f"http://{os.getenv('ELASTICSEARCH_HOST')}:{os.getenv('ELASTICSEARCH_PORT')}"
    logging.WARN(f'Connecting to Elasticsearch: {elasticsearch_url}')
    logging.WARN(f'Password: {os.getenv("ELASTICSEARCH_PASSWORD")}')
    client = Elasticsearch(
        [elasticsearch_url],
        basic_auth=('elastic', os.getenv('ELASTICSEARCH_PASSWORD'))
    )
    mapping = {
        "mappings": {
            "properties": {
                "spans": {
                    "type": "nested"
                }
            }
        }
    }
    if not client.indices.exists(index=os.getenv('ELASTICSEARCH_INDEX')):
        client.indices.create(index=os.getenv('ELASTICSEARCH_INDEX'), body=mapping)
    return client
