import logging
import os
import time

from crud import RedisRepository
from dto.Work import Work
from service import ErrorTraceProcessor


class RedisWorker:
    def __init__(self,
                 fast_interval=int(os.getenv('REDIS_FAST_INTERVAL')),
                 slow_interval=int(os.getenv('REDIS_SLOW_INTERVAL')),
                 fast_que=os.getenv('REDIS_FAST_QUE'),
                 slow_que=os.getenv('REDIS_SLOW_QUE')
                 ):
        self.fast_interval = fast_interval
        self.slow_interval = slow_interval
        self.fast_que = fast_que
        self.slow_que = slow_que

    def run_fast_queue(self):
        while True:
            work = RedisRepository.dequeue_data(self.fast_que)
            if work is None:
                time.sleep(self.fast_interval)
                continue
            logging.info(f'[RedisWorker] run_fast_queue -> START: polled data {work}')
            result = ErrorTraceProcessor.process_work(work)
            if not result:
                if work.count >= 5:
                    self.__to_slow_queue(work)
                else:
                    RedisRepository.enqueue_data(work, self.fast_que)
            time.sleep(self.fast_interval)

    def run_slow_queue(self):
        while True:
            work = RedisRepository.dequeue_data(self.slow_que)
            if work is None:
                time.sleep(self.slow_interval)
                continue
            result = ErrorTraceProcessor.process_work(work)
            if not result and work.count < 60:
                    RedisRepository.enqueue_data(work, self.slow_que)
            time.sleep(self.slow_interval)

    def __to_slow_queue(self, data: Work):
        logging.info(f'[RedisWorker] __to_slow_queue -> START: {data}')
        RedisRepository.enqueue_data(data, self.slow_que)
        logging.info(f'[RedisWorker] __to_slow_queue -> END: {data}')
