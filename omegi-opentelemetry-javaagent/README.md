## omegi-opentelemtety-javaagent 프로젝트 빌드

Gradle-Tasks-other-extendedAgent 실행
build/libs 경로에 omegi-opentelemtety-javaagent.jar 생성됨

## Class 설명

DemoAutoConfigurationCustomizerProvider : javaagent에서 customize 한 클래스를 사용하기 위한 명시 클래스

OmegiExtendedInstrumentation : Additional helper class로 메소드 enter/exit을 파악해 span 생성

OmegiExtendedInstrumentationModule : OmegiExtendedInstrumentation 등록 클래스

OmegiLogRecordExporter : log를 전송하는 custom exporter(추후 없애거나 커스텀 log 찍는 용으로 변경할 예정)

OmegiLogRecordExporterFactory : OmegiLogRecordExporter 등록 클래스

OmegiTraceRecordExporter : kafka로 trace를 전송하는 custom exporter

OmegiTraceRecordExporterFactory : OmegiTraceRecordExporter 등록 클래스

## META-INF/services

custom한 Exporter/instrumentation 명시

## data

```
"MyService.MyService":b0c1645645dd9939186ebe35e5faa629 5889092a370d58e5 INTERNAL[
   "tracer":"omegi":"omegi":1.0.0
]"AttributesMap"{
   "data="{
      thread.name=http-nio-9090-exec-1,
      thread.id=32,
      method=org.omegi.test.MyService.testMethod2,
      "param="[
         
      ]
   },
   capacity=128,
   totalAddedValues=4
}"GET /test":b0c1645645dd9939186ebe35e5faa629 c134602379d8d67c SERVER[
   "tracer":"io.opentelemetry.tomcat-10.0":2.4.0-alpha-SNAPSHOT
]"AttributesMap"{
   "data="{
      server.port=9090,
      thread.id=32,
      http.response.status_code=200,
      "http.route=/test",
      "http.request.method=GET",
      "url.path=/test",
      "server.address=localhost",
      client.address=172.18.0.1,
      network.peer.address=172.18.0.1,
      "url.scheme=http",
      thread.name=http-nio-9090-exec-1,
      network.peer.port=51978,
      network.protocol.version=1.1,
      user_agent.original=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,
      like Gecko) Chrome/124.0.0.0 Safari/537.36
   },
   capacity=128,
   totalAddedValues=14
}
```
key value 형식으로 들어오고 있음

## 수정 계획

build.gradle 의존성 정리

error 발생 로그만 kafka로 전송하도록 수정

trace 전송 형식 수정

