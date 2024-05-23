# import logging
#
# # logging.basicConfig(level=logging.INFO)
#
# import threading
# import time
#
# from test.TestRedisWorker import RedisWorker
# from dotenv import load_dotenv
# from test import TestProducer
# from test.TestConsumer import ErrorConsumer
#
# load_dotenv()
# # TestProducer.make_data()
# consumer = ErrorConsumer()
#
#
# def main():
#     global consumer
#     print("Starting Collect-server", flush=True)
#     # flow_consumer = FlowConsumer()
#     redis_worker = RedisWorker()
#
#     consumer_thread = threading.Thread(target=consumer.activate_listener)
#     # flow_consumer_thread = threading.Thread(target=flow_consumer.activate_listener)
#     redis_worker_fast_thread = threading.Thread(target=redis_worker.run_fast_queue)
#     redis_worker_slow_thread = threading.Thread(target=redis_worker.run_slow_queue)
#
#     consumer_thread.start()
#     # flow_consumer_thread.start()
#     redis_worker_fast_thread.start()
#     redis_worker_slow_thread.start()
#
#     try:
#         while True:
#             # Sleep for 3 seconds
#             time.sleep(3)
#
#             # Print the total consumed messages
#             print(f"Total messages consumed: {consumer.get_consumed_count()}", flush=True)
#
#     except KeyboardInterrupt:
#         print("Stopping Collect-server")
#
#     consumer_thread.join()
#     # flow_consumer_thread.join()
#     redis_worker_fast_thread.join()
#     redis_worker_slow_thread.join()
#
#
# main()
