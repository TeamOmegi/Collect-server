import os
from datetime import datetime

from kafka import KafkaProducer
import json


def make_data():
    producer = KafkaProducer(bootstrap_servers=[f'{os.getenv("KAFKA_HOST_1")}:{os.getenv("KAFKA_PORT")}',
                                                f'{os.getenv("KAFKA_HOST_2")}:{os.getenv("KAFKA_PORT")}'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for i in range(500000):
        message = {
            "tracer": "omegi-tracer-python",
            "traceId": "6aaa192456e549c23587ad92e7d29916",
            "token": "eyJhbGciOiJIUzI1NiJ9.eyJwcm9qZWN0SWQiOjMsInNlcnZpY2VJZCI6MjAsImlhdCI6MTcxNjEyNTQxMX0.GGW3VYvyYpn3yI1zK_AW4oA4aZY8niH_d04bQyykNNA",
            "serviceName": "AI-Server",
            "error": {
                "exception.type": "TypeError",
                "exception.message": "missing a required argument: 'error'",
                "exception.flow": {
                    "exception.flow": {
                        "step.1": "usr.local.libthon3.10.inspect._bind",
                        "step.2": "usr.local.libthon3.10.inspect.bind",
                        "step.3": "usr.local.libthon3.10.site-packages.omegi.decorator.OmegiDecorator.wrapper",
                        "step.4": "app.mock_service.MockService.mock_method_a",
                        "step.5": "usr.local.libthon3.10.site-packages.omegi.decorator.OmegiDecorator.wrapper",
                        "step.6": "app.main.python_a_error",
                        "step.7": "usr.local.libthon3.10.site-packages.fastapi.routing.run_endpoint_function",
                        "step.8": "usr.local.libthon3.10.site-packages.fastapi.routing.app",
                        "step.9": "usr.local.libthon3.10.site-packages.starlette.routing.app",
                        "step.10": "usr.local.libthon3.10.site-packages.starlette._exception_handler.wrapped_app",
                        "step.11": "usr.local.libthon3.10.site-packages.starlette._exception_handler.wrapped_app",
                        "step.12": "usr.local.libthon3.10.site-packages.starlette.routing.app",
                        "step.13": "usr.local.libthon3.10.site-packages.starlette.routing.handle",
                        "step.14": "usr.local.libthon3.10.site-packages.starlette.routing.app",
                        "step.15": "usr.local.libthon3.10.site-packages.starlette.routing.__call__",
                        "step.16": "usr.local.libthon3.10.site-packages.starlette._exception_handler.wrapped_app",
                        "step.17": "usr.local.libthon3.10.site-packages.starlette._exception_handler.wrapped_app",
                        "step.18": "usr.local.libthon3.10.site-packages.starlette.middleware.exceptions.__call__",
                        "step.19": "usr.local.libthon3.10.site-packages.opentelemetry.instrumentation.asgi.__init__.__call__",
                        "step.20": "usr.local.libthon3.10.site-packages.opentelemetry.trace.__init__.use_span"
                    }
                },
                "exception.stacktrace": "Traceback (most recent call last):\n  File \"/usr/local/lib/python3.10/site-packages/opentelemetry/trace/__init__.py\", line 570, in use_span\n    yield span\n  File \"/usr/local/lib/python3.10/site-packages/opentelemetry/instrumentation/asgi/__init__.py\", line 631, in __call__\n    await self.app(scope, otel_receive, otel_send)\n  File \"/usr/local/lib/python3.10/site-packages/starlette/middleware/exceptions.py\", line 65, in __call__\n    await wrap_app_handling_exceptions(self.app, conn)(scope, receive, send)\n  File \"/usr/local/lib/python3.10/site-packages/starlette/_exception_handler.py\", line 64, in wrapped_app\n    raise exc\n  File \"/usr/local/lib/python3.10/site-packages/starlette/_exception_handler.py\", line 53, in wrapped_app\n    await app(scope, receive, sender)\n  File \"/usr/local/lib/python3.10/site-packages/starlette/routing.py\", line 756, in __call__\n    await self.middleware_stack(scope, receive, send)\n  File \"/usr/local/lib/python3.10/site-packages/starlette/routing.py\", line 776, in app\n    await route.handle(scope, receive, send)\n  File \"/usr/local/lib/python3.10/site-packages/starlette/routing.py\", line 297, in handle\n    await self.app(scope, receive, send)\n  File \"/usr/local/lib/python3.10/site-packages/starlette/routing.py\", line 77, in app\n    await wrap_app_handling_exceptions(app, request)(scope, receive, send)\n  File \"/usr/local/lib/python3.10/site-packages/starlette/_exception_handler.py\", line 64, in wrapped_app\n    raise exc\n  File \"/usr/local/lib/python3.10/site-packages/starlette/_exception_handler.py\", line 53, in wrapped_app\n    await app(scope, receive, sender)\n  File \"/usr/local/lib/python3.10/site-packages/starlette/routing.py\", line 72, in app\n    response = await func(request)\n  File \"/usr/local/lib/python3.10/site-packages/fastapi/routing.py\", line 278, in app\n    raw_response = await run_endpoint_function(\n  File \"/usr/local/lib/python3.10/site-packages/fastapi/routing.py\", line 191, in run_endpoint_function\n    return await dependant.call(**values)\n  File \"/app/main.py\", line 20, in python_a_error\n    MockService.mock_method_a(user_name, user_major, user_friends)\n  File \"/usr/local/lib/python3.10/site-packages/omegi/decorator/OmegiDecorator.py\", line 24, in wrapper\n    return func(*args, **kwargs)\n  File \"/app/mock_service/MockService.py\", line 6, in mock_method_a\n    mock_method_b()\n  File \"/usr/local/lib/python3.10/site-packages/omegi/decorator/OmegiDecorator.py\", line 11, in wrapper\n    bound_args = signature.bind(*args, **kwargs)\n  File \"/usr/local/lib/python3.10/inspect.py\", line 3186, in bind\n    return self._bind(args, kwargs)\n  File \"/usr/local/lib/python3.10/inspect.py\", line 3101, in _bind\n    raise TypeError(msg) from None\nTypeError: missing a required argument: 'error'\n"
            },
            "spans": [
                {
                    "name": "mock_service.MockService.mock_method_a",
                    "spanId": "102942e4e841e787",
                    "parentSpanId": "68275bae4489487c",
                    "kind": "INTERNAL",
                    "spanEnterTime": "2024-05-23 01:42:17.233",
                    "spanExitTime": "2024-05-23 01:42:17.281",
                    "attributes": {
                        "module": "mock_service.MockService",
                        "name": "mock_method_a",
                        "thread.name": "MainThread",
                        "thread.id": "140588693015424",
                        "arguments": "('user_name: str = 김싸피', 'user_major: str = 컴퓨터공학과', \"user_friends: list = ['강싸피', '이싸피', '박싸피']\")"
                    }
                },
                {
                    "name": "GET /python-a/error",
                    "spanId": "68275bae4489487c",
                    "parentSpanId": "de108c8907941155",
                    "kind": "SERVER",
                    "spanEnterTime": "2024-05-23 01:42:17.077",
                    "spanExitTime": "2024-05-23 01:42:17.290",
                    "attributes": {
                        "http.scheme": "http",
                        "http.host": "172.18.0.6:8091",
                        "net.host.port": "8091",
                        "http.flavor": "1.1",
                        "http.target": "/python-a/error",
                        "http.url": "http://172.18.0.6:8091/python-a/error?user_name=김싸피&user_major=컴퓨터공학과",
                        "http.method": "GET",
                        "http.server_name": "python-a:8091",
                        "http.user_agent": "ReactorNetty/1.1.18",
                        "net.peer.ip": "172.18.0.4",
                        "net.peer.port": "58718",
                        "http.route": "/python-a/error"
                    }
                }
            ]
        }
        producer.send(os.getenv("KAFKA_LOG_TOPIC"), value=message)

        message = {
            "tracer": "io.opentelemetry.spring-webflux-5.3:2.4.0-alpha-SNAPSHOT",
            "traceId": "6aaa192456e549c23587ad92e7d29916",
            "token": "eyJhbGciOiJIUzI1NiJ9.eyJwcm9qZWN0SWQiOjMsInNlcnZpY2VJZCI6MTgsImlhdCI6MTcxNjEyNTQwOH0.MohkaBxK1lx9Wfz_4wH0BQsMlyPUUAQkvohQuSlONwM",
            "serviceName": "Main-Server",
            "error": {},
            "spans": [
                {
                    "name": "GET",
                    "spanId": "de108c8907941155",
                    "parentSpanId": "d9143a7e283385b9",
                    "kind": "CLIENT",
                    "spanEnterTime": "2024-05-23 10:42:15.902",
                    "spanExitTime": "2024-05-23 10:42:17.365",
                    "attributes": {
                        "http.request.method": "GET",
                        "server.port": 8091,
                        "server.address": "python-a",
                        "http.response.status_code": 500,
                        "error.type": "500",
                        "url.full": "http://python-a:8091/python-a/error?user_name=%EA%B9%80%EC%8B%B8%ED%94%BC&user_major=%EC%BB%B4%ED%93%A8%ED%84%B0%EA%B3%B5%ED%95%99%EA%B3%BC"
                    }
                },
                {
                    "name": "org.omegi.mockjavaa.service.WebClientService.toPythonAError",
                    "spanId": "d9143a7e283385b9",
                    "parentSpanId": "b1799fe1a681f39c",
                    "kind": "INTERNAL",
                    "spanEnterTime": "2024-05-23 10:42:15.611",
                    "spanExitTime": "2024-05-23 10:42:17.414",
                    "attributes": {
                        "arguments": "[java.lang.String userName : 김싸피, java.lang.String userSchool : 싸피대학교, java.lang.String userMajor : 컴퓨터공학과, java.util.Map<java.lang.String, java.lang.Double> gradeMap : {운영체제=4.5, 분산처리시스템=3.5, 데이터마이닝=4.0, 컴퓨터보안=4.0}]"
                    }
                },
                {
                    "name": "org.omegi.mockjavaa.service.MockServiceJavaA.toPythonAErrorJavaA",
                    "spanId": "b1799fe1a681f39c",
                    "parentSpanId": "13d975b9b7d85916",
                    "kind": "INTERNAL",
                    "spanEnterTime": "2024-05-23 10:42:15.606",
                    "spanExitTime": "2024-05-23 10:42:17.414",
                    "attributes": {
                        "arguments": "[java.lang.String userName : 김싸피, int userAge : 22, java.lang.String userSchool : 싸피대학교]"
                    }
                },
                {
                    "name": "GET /java-a/python-a/error",
                    "spanId": "13d975b9b7d85916",
                    "parentSpanId": "0000000000000000",
                    "kind": "SERVER",
                    "spanEnterTime": "2024-05-23 10:42:15.595",
                    "spanExitTime": "2024-05-23 10:42:17.494",
                    "attributes": {
                        "network.peer.port": 51385,
                        "network.protocol.version": "1.1",
                        "user_agent.original": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                        "url.scheme": "http",
                        "http.request.method": "GET",
                        "server.port": 8081,
                        "http.route": "/java-a/python-a/error",
                        "server.address": "3.34.222.192",
                        "client.address": "211.192.210.243",
                        "http.response.status_code": 500,
                        "network.peer.address": "211.192.210.243",
                        "url.path": "/java-a/python-a/error",
                        "error.type": "500"
                    }
                }
            ]
        }
        producer.send(os.getenv("KAFKA_LOG_TOPIC"), value=message)

    producer.flush()
    producer.close()
    print("데이터 1000000개 발행 완료", flush=True)
    time = datetime.now()
    print(f'발행 시간 : {time}', flush=True)
