package org.omegi.omegiextension;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.traces.ConfigurableSpanExporterProvider;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class OmegiTraceSpanExporterFactory implements ConfigurableSpanExporterProvider {

    @Override
    public SpanExporter createExporter(ConfigProperties configProperties) {
        OmegiTraceSpanExporter exporter = OmegiTraceSpanExporter.create();
        return exporter;
    }

    @Override
    public String getName() {
        return "OmegiTraceSpanExporter";
    }
}