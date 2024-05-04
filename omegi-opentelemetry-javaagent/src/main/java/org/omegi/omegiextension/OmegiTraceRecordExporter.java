package org.omegi.omegiextension;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class OmegiTraceRecordExporter implements SpanExporter {

    private static final Logger logger = Logger.getLogger(OmegiTraceRecordExporter.class.getName());
    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private final KafkaProducer<String, String> kafkaProducer;

    public OmegiTraceRecordExporter() {
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

    public static OmegiTraceRecordExporter create() {
        return new OmegiTraceRecordExporter();
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        logger.info("들어왔나1111121212");
        if (this.isShutdown.get()) {
            logger.info("들어왔21212나");
            return CompletableResultCode.ofFailure();
        } else {
            logger.info("들어왔44441나");
            StringBuilder sb = new StringBuilder(60);
            Iterator var3 = spans.iterator();

            while (var3.hasNext()) {
                SpanData span = (SpanData) var3.next();
                InstrumentationScopeInfo instrumentationScopeInfo = span.getInstrumentationScopeInfo();
                sb.append("'").append(span.getName()).append("' : ").append(span.getTraceId()).append(" ")
                        .append(span.getSpanId()).append(" ").append(span.getKind()).append(" [tracer: ")
                        .append(instrumentationScopeInfo.getName()).append(":")
                        .append(instrumentationScopeInfo.getVersion() == null ? "" : instrumentationScopeInfo.getVersion())
                        .append("] ").append(span.getAttributes());
            }
            logger.info(sb.toString());
            ProducerRecord<String, String> record = new ProducerRecord<>("test", sb.toString());
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
        if (!this.isShutdown.compareAndSet(false, true)) {
            logger.log(Level.INFO, "Calling shutdown() multiple times.");
            return CompletableResultCode.ofSuccess();
        } else {
            return this.flush();
        }
    }
}