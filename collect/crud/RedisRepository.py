from database.RedisClient import get_redis_client
from dto.Work import Work


redis_client = get_redis_client()


def enqueue_data(data: Work, que_name):
    data.error_trace = None
    data.count += 1
    data_json = data.json()
    redis_client.rpush(que_name, data_json)


def dequeue_data(que_name) -> Work:
    data = redis_client.lpop(que_name)
    if data:
        return Work.parse_raw(data)
    return None


work = Work(trace_id='dd', project_id=1, service_id=1, count = 0, error_trace=None)

enqueue_data(work, "fast_queue")
