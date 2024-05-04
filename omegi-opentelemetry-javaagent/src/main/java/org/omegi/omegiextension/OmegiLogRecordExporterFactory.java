package org.omegi.omegiextension;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.logs.ConfigurableLogRecordExporterProvider;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;

public class OmegiLogRecordExporterFactory implements ConfigurableLogRecordExporterProvider {

	@Override
	public LogRecordExporter createExporter(ConfigProperties configProperties) {
		OmegiLogRecordExporter exporter = OmegiLogRecordExporter.create();
		return exporter;
	}

	@Override
	public String getName() {
		return "OmegiLogRecordExporter";
	}
}