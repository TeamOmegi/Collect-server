import redis
from dotenv import load_dotenv
import os

load_dotenv()


def get_redis_client():
    return redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),
        decode_responses=True)

