FROM openjdk:17-jdk-alpine

WORKDIR /app

# 빌드 로직 넣어주면 너무 느려서 & 수정될 일 없어서 그냥 수동 빌드 하고 찾아오는 걸로 했음
COPY /build/libs/test-0.0.1-SNAPSHOT.jar /app/app.jar

# omegi-opentelemetry-javaagent.jar path 현재 경로로 되어있는데 상황 맞게 수정해야 함
COPY omegi-opentelemetry-javaagent.jar /app/omegi-opentelemetry-javaagent.jar

CMD ["java", \
     "-javaagent:omegi-opentelemetry-javaagent.jar", \
     "-Dotel.exporter.omegi.token=token", \
     "-Dotel.traces.exporter=OmegiTraceSpanExporter", \
     "-Dotel.logs.exporter=none", \
     "-Dotel.metrics.exporter=none", \
     "-Dotel.service.name=service-name", \
     "-jar", "app.jar"]

# 실행 명령
# docker build -t app:latest . # 이미지 빌드
# docker run -d --name app --network test-net -p 8080:8080 app:latest