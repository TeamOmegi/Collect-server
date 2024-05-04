package org.omegi.omegiextension;

import com.google.auto.service.AutoService;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.logs.SdkLoggerProviderBuilder;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

@AutoService(AutoConfigurationCustomizerProvider.class)
public class DemoAutoConfigurationCustomizerProvider
        implements AutoConfigurationCustomizerProvider {

    @Override
    public void customize(AutoConfigurationCustomizer autoConfiguration) {
        autoConfiguration
                .addTracerProviderCustomizer(this::configureSdkTracerProvider)
                .addLoggerProviderCustomizer(this::configureSdkLogProvider);
    }

    private SdkTracerProviderBuilder configureSdkTracerProvider(
            SdkTracerProviderBuilder tracerProvider, ConfigProperties config) {

        return tracerProvider
                .addSpanProcessor(SimpleSpanProcessor.create(new OmegiTraceRecordExporter()));
    }

    private SdkLoggerProviderBuilder configureSdkLogProvider(
            SdkLoggerProviderBuilder loggerProvider, ConfigProperties config) {

        return loggerProvider
                .addLogRecordProcessor(SimpleLogRecordProcessor.create(new OmegiLogRecordExporter()));
    }
}