package org.omegi.omegiextension;

import com.google.auto.service.AutoService;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanLimits;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.time.Duration;
import java.time.Instant;

@AutoService(AutoConfigurationCustomizerProvider.class)
public class DemoAutoConfigurationCustomizerProvider
	implements AutoConfigurationCustomizerProvider {

	private static final Instant START_TIME = Instant.now();
	private static final double MIN_SAMPLING_RATE = 0.1;

	@Override
	public void customize(AutoConfigurationCustomizer autoConfiguration) {
		autoConfiguration
			.addTracerProviderCustomizer(this::combinedTracerProviderCustomizer);
	}

	private SdkTracerProviderBuilder combinedTracerProviderCustomizer(
		SdkTracerProviderBuilder tracerProvider, ConfigProperties config) {
		return configureSdkTracerProvider(sampleConfigureSdkTracerProvider(tracerProvider, config), config);
	}

	private SdkTracerProviderBuilder sampleConfigureSdkTracerProvider(
		SdkTracerProviderBuilder tracerProvider, ConfigProperties config) {
		Duration elapsedTime = Duration.between(START_TIME, Instant.now());
		long elapsedDays = elapsedTime.toDays();
		double samplingRate = Math.max(MIN_SAMPLING_RATE, Math.pow(0.5, elapsedDays / 7.0)); // 최소 0.1로 제한

		return SdkTracerProvider.builder()
			.setClock(Clock.getDefault())
			.setIdGenerator(IdGenerator.random())
			.setResource(Resource.getDefault())
			.setSampler(Sampler.traceIdRatioBased(samplingRate))
			.setSpanLimits(SpanLimits.getDefault())
			.addSpanProcessor(BatchSpanProcessor.builder(new SampleOmegiTraceSpanExporter()).build());
	}

	private SdkTracerProviderBuilder configureSdkTracerProvider(
		SdkTracerProviderBuilder tracerProvider, ConfigProperties config) {
		return tracerProvider
			.addSpanProcessor(BatchSpanProcessor.builder(new OmegiTraceSpanExporter()).build());
	}
}