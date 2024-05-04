package org.omegi.omegiextension;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class OmegiLogRecordExporter implements LogRecordExporter {

    private static final DateTimeFormatter ISO_FORMAT;
    private static final Logger logger = Logger.getLogger(OmegiLogRecordExporter.class.getName());
    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final KafkaProducer<String, String> kafkaProducer;

    public OmegiLogRecordExporter() {
        this.kafkaProducer = createKafkaProducer();
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    public static OmegiLogRecordExporter create() {
        return new OmegiLogRecordExporter();
    }

    @Override
    public CompletableResultCode export(Collection<LogRecordData> logs) {
        logger.info("들어왔나");
        if (this.isShutdown.get()) {
            logger.info("들어왔나1111");
            return CompletableResultCode.ofFailure();
        } else {
            logger.info("들어왔나22222");
            StringBuilder stringBuilder = new StringBuilder(60);
            Iterator var3 = logs.iterator();

            while (var3.hasNext()) {
                LogRecordData log = (LogRecordData) var3.next();
                formatLog(stringBuilder, log);
            }
            logger.info(stringBuilder.toString());
            ProducerRecord<String, String> record = new ProducerRecord<>("test", stringBuilder.toString());
            kafkaProducer.send(record);
            return CompletableResultCode.ofSuccess();
        }
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        logger.info("들어왔나44444");
        if (!this.isShutdown.compareAndSet(false, true)) {
            logger.log(Level.INFO, "Calling shutdown() multiple times.");
            return CompletableResultCode.ofSuccess();
        } else {
            return this.flush();
        }
    }

    static void formatLog(StringBuilder stringBuilder, LogRecordData log) {
        InstrumentationScopeInfo instrumentationScopeInfo = log.getInstrumentationScopeInfo();
        stringBuilder.append(ISO_FORMAT.format(
                        Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(log.getTimestampEpochNanos())).atZone(ZoneOffset.UTC)))
                .append(" ").append(log.getSeverity()).append(" '").append(log.getBody().asString()).append("' : ")
                .append(log.getSpanContext().getTraceId()).append(" ").append(log.getSpanContext().getSpanId())
                .append(" [scopeInfo: ").append(instrumentationScopeInfo.getName()).append(":")
                .append(instrumentationScopeInfo.getVersion() == null ? "" : instrumentationScopeInfo.getVersion())
                .append("] ").append(log.getAttributes());
    }

    static {
        ISO_FORMAT = DateTimeFormatter.ISO_DATE_TIME;
    }
}