import logging

from database.RedisClient import get_redis_client
from dto.Work import Work


redis_client = get_redis_client()


def enqueue_data(data: Work, que_name):
    data.error_trace = None
    data.count += 1
    data_json = data.json()
    logging.info(f'[RedisRepository] enqueue_data -> START: put data {data_json}')
    redis_client.rpush(que_name, data_json)


def dequeue_data(que_name) -> Work:
    data = redis_client.lpop(que_name)
    if data:
        logging.info(f'[RedisRepository] dequeue_data -> END: returned data {data}')
        return Work.parse_raw(data)
    return None

