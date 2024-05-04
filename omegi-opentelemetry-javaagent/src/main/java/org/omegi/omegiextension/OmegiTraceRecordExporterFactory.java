package org.omegi.omegiextension;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.traces.ConfigurableSpanExporterProvider;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class OmegiTraceRecordExporterFactory implements ConfigurableSpanExporterProvider {

    @Override
    public SpanExporter createExporter(ConfigProperties configProperties) {
        OmegiTraceRecordExporter exporter = OmegiTraceRecordExporter.create();
        return exporter;
    }

    @Override
    public String getName() {
        return "OmegiTraceRecordExporter";
    }
}