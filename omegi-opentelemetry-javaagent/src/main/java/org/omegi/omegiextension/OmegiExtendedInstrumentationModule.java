package org.omegi.omegiextension;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
@AutoService({InstrumentationModule.class})
public final class OmegiExtendedInstrumentationModule extends InstrumentationModule {

	public OmegiExtendedInstrumentationModule() {
		super("omegi-extended", new String[]{"omegiextended"});
	}

	@Override
	public int order() {
		return 1;
	}

	@Override
	public List<String> getAdditionalHelperClassNames() {
		return Arrays.asList(
				OmegiExtendedInstrumentation.class.getName(),
				"io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation"
		);
	}

	@Override
	public List<TypeInstrumentation> typeInstrumentations() {
		return Collections.singletonList(new OmegiExtendedInstrumentation());
	}


}
