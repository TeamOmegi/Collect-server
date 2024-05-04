from entity.ErrorLog import ErrorLog
from crud import MongoRepository


# 에러 로그 원본 가공 후 저장
def insert_error_log(message: str):
    error_log = process_error_log(message)
    MongoRepository.insert(error_log)


# 에러 로그 원본 가공
def process_error_log(log: str) -> ErrorLog:
    return ErrorLog(
        project="hello",
        service="test",
        log=log
    )


# 에러 로그로 서비스 흐름 가공
def process_error_to_service_flow(log: str):
    print(log)
