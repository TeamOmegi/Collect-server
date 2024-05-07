package org.omegi.omegiextension;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.omegi.omegiextension.util.OmegiUtil;

public class SampleOmegiTraceSpanExporter implements SpanExporter {

	private static final Logger logger = Logger.getLogger(SampleOmegiTraceSpanExporter.class.getName());
	private final AtomicBoolean isShutdown = new AtomicBoolean();
	private final Gson gson = new Gson();
	private final KafkaProducer<String, byte[]> kafkaProducer;

	public SampleOmegiTraceSpanExporter() {
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

	public static SampleOmegiTraceSpanExporter create() {
		return new SampleOmegiTraceSpanExporter();
	}

	@Override
	public CompletableResultCode export(Collection<SpanData> spans) {
		if (isShutdown.get()) {
			return CompletableResultCode.ofFailure();
		}

		JsonObject outerJson = new JsonObject();
		SpanData firstSpan = spans.stream().findFirst().orElse(null);

		if (firstSpan != null) {
			StatusCode statusCode = firstSpan.getStatus().getStatusCode();
			if (statusCode == StatusCode.ERROR) {
				return CompletableResultCode.ofSuccess();
			}
		}

		InstrumentationScopeInfo instrumentationScopeInfo = firstSpan.getInstrumentationScopeInfo();
		String traceId = firstSpan.getTraceId();
		outerJson.addProperty("tracer",
			instrumentationScopeInfo.getName() + ":" + (instrumentationScopeInfo.getVersion() == null ? ""
				: instrumentationScopeInfo.getVersion()));
		outerJson.addProperty("traceId", traceId);
		outerJson.addProperty("token", OmegiUtil.getToken());
		outerJson.addProperty("service-name", OmegiUtil.getServiceName());

		JsonObject jsonData = new JsonObject();
		SpanData span = spans.stream().reduce((first, second) -> second).orElse(null);

		if (span != null) {
			jsonData.addProperty("name", span.getName());
			jsonData.addProperty("spanId", span.getSpanId());
			jsonData.addProperty("parentSpanId", span.getParentSpanId());
			jsonData.addProperty("kind", span.getKind().toString());
			jsonData.addProperty("span enter-time", OmegiUtil.getFormattedTime(span.getStartEpochNanos()));
			jsonData.addProperty("span exit-time", OmegiUtil.getFormattedTime(span.getEndEpochNanos()));
			jsonData.add("attributes", gson.toJsonTree(span.getAttributes()));

			outerJson.add("spans", jsonData);
		}

		ProducerRecord<String, byte[]> record = new ProducerRecord<>("flow",
			outerJson.toString().getBytes(StandardCharsets.UTF_8));
		kafkaProducer.send(record);
		return CompletableResultCode.ofSuccess();
	}

	@Override
	public CompletableResultCode flush() {
		return CompletableResultCode.ofSuccess();
	}

	@Override
	public CompletableResultCode shutdown() {
		return CompletableResultCode.ofSuccess();
	}
}