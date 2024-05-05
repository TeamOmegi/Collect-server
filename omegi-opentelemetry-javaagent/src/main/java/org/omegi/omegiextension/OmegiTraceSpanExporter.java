package org.omegi.omegiextension;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class OmegiTraceSpanExporter implements SpanExporter {

    private static final Logger logger = Logger.getLogger(OmegiTraceSpanExporter.class.getName());
    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final Gson gson = new Gson();
    private final KafkaProducer<String, byte[]> kafkaProducer;

    public OmegiTraceSpanExporter() {
        this.kafkaProducer = createKafkaProducer();
    }

    private static KafkaProducer<String, byte[]> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(properties);
    }

    public static OmegiTraceSpanExporter create() {
        return new OmegiTraceSpanExporter();
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        logger.info("export 진입");
        if (isShutdown.get()) {
            return CompletableResultCode.ofFailure();
        }

        /*
        에러 데이터면 error info 생성
        에러가 아니면 종료
         */
        JsonObject outerJson = new JsonObject();
        String traceEnterTime = null;
        String traceExitTime = null;

        SpanData firstSpan = spans.stream().findFirst().orElse(null);

        if (firstSpan != null) {
            StatusCode statusCode = firstSpan.getStatus().getStatusCode();
            if (statusCode != StatusCode.ERROR) {
                return CompletableResultCode.ofSuccess();
            } else {
                InstrumentationScopeInfo instrumentationScopeInfo = firstSpan.getInstrumentationScopeInfo();

                if (!instrumentationScopeInfo.getName().startsWith("omegi")) {
                    return CompletableResultCode.ofSuccess();
                }

                String traceId = firstSpan.getTraceId();
                outerJson.addProperty("tracer", instrumentationScopeInfo.getName() + ":" + (instrumentationScopeInfo.getVersion() == null ? "" : instrumentationScopeInfo.getVersion()));
                outerJson.addProperty("traceId", traceId);

                JsonObject exceptionJson = new JsonObject();

                if (firstSpan.getEvents() != null && firstSpan.getEvents().size() > 0) {
                    EventData eventData = firstSpan.getEvents().get(0);
                    String stacktrace = eventData.getAttributes().get(AttributeKey.stringKey("exception.stacktrace"));
                    exceptionJson.addProperty("exception.type", eventData.getAttributes().get(AttributeKey.stringKey("exception.type")));
                    exceptionJson.addProperty("exception.message", eventData.getAttributes().get(AttributeKey.stringKey("exception.message")));

                    JsonObject stackFlowData = new JsonObject();
                    List<String> stacktraceList = analyzeErrorFlow(stacktrace);

                    int flowNumber = 1;
                    for (String traced : stacktraceList) {
                        stackFlowData.addProperty("step." + flowNumber++, traced);
                    }

                    exceptionJson.add("exception.flow", stackFlowData);
                    exceptionJson.addProperty("exception.stacktrace", stacktrace);
                }

                outerJson.add("error", exceptionJson);
            }
        }

        /*
        상세 span 정보
         */

        int spanNumber = 1;
        for (SpanData span : spans) {
            JsonObject jsonData = new JsonObject();
            jsonData.addProperty("name", span.getName());
            jsonData.addProperty("spanId", span.getSpanId());
            jsonData.addProperty("parentSpanId", span.getParentSpanId());
            jsonData.addProperty("kind", span.getKind().toString());
            jsonData.addProperty("span enter-time", getFormattedTime(span.getStartEpochNanos()));
            jsonData.addProperty("span exit-time", getFormattedTime(span.getEndEpochNanos()));
            jsonData.add("attributes", gson.toJsonTree(span.getAttributes()));

            if (spans.size() == spanNumber) {
                traceEnterTime = getFormattedTime(span.getStartEpochNanos());
                traceExitTime = getFormattedTime(span.getEndEpochNanos());
            }

            outerJson.add("detailed-span" + spanNumber++, jsonData);
        }

        outerJson.addProperty("trace enter-time", traceEnterTime);
        outerJson.addProperty("trace exit-time", traceExitTime);

        /*
        카프카 전송
         */
        ProducerRecord<String, byte[]> record = new ProducerRecord<>("test", outerJson.toString().getBytes(StandardCharsets.UTF_8));
        kafkaProducer.send(record);
        return CompletableResultCode.ofSuccess();
    }

    /*
    시간 타입 변환(우리나라 기준)
     */
    private String getFormattedTime(long time) {
        Instant instant = Instant.ofEpochSecond(time / 1_000_000_000L, time % 1_000_000_000L);
        ZoneId zoneId = ZoneId.of("Asia/Seoul");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(zoneId);
        return formatter.format(instant);
    }

    /*
    stacktrace로  error flow 생성
     */
    private List<String> analyzeErrorFlow(String errorMessage) {
        List<String> errorFlow = new ArrayList<>();
        Pattern pattern = Pattern.compile("at (.*?)\\.([^.]*?)\\.([^.]*?)\\(");
        Matcher matcher = pattern.matcher(errorMessage);

        while (matcher.find()) {
            String packageName = matcher.group(1);
            String className = matcher.group(2);
            String methodName = matcher.group(3);

            String step = packageName + "." + className + "." + methodName;
            errorFlow.add(step);
        }

        return errorFlow;
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        if (!this.isShutdown.compareAndSet(false, true)) {
            logger.log(Level.INFO, "Calling shutdown() multiple times.");
            return CompletableResultCode.ofSuccess();
        } else {
            return this.flush();
        }
    }
}