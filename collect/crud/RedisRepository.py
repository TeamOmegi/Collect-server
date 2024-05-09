from database.RedisClient import get_redis_client
from dto.Work import Work


redis_client = get_redis_client()


def enqueue_data(data: Work, que_name):
    data_json = data.json()
    redis_client.rpush(que_name, data_json)


def dequeue_data(que_name) -> Work:
    data = redis_client.lpop(que_name)
    return Work.parse_raw(data)
