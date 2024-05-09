import logging
import os
import time

from crud import ElasticSearchRepository
from crud import RedisRepository
from dto.Work import Work


class FastPollingWorker():
    def __init__(self, interval=os.getenv('REDIS_FAST_INTERVAL'), que_name=os.getenv('REDIS_FAST_QUE')):
        self.interval = interval
        self.que_name = que_name

    def run(self):
        while True:
            data = RedisRepository.dequeue_data(self.que_name)
            # ElasticSearch에서 찾아보기
            error_trace = self.get_error_trace(data)
                # 정상적으로 처리되지 않은 경우
                # 횟수 +1 5회 째면 long으로

            time.sleep(self.interval)



    def to_slow_queue(self, data: Work):
        logging.info(f'[FastPollingWorker] Que Transfer-start: fast_que to slow_que {data}')
        RedisRepository.enqueue_data(data, os.getenv('REDIS_SLOW_QUE'))
        logging.info(f'[FastPollingWorker] Que Transfer-end: fast_que to slow_que {data}')