package org.omegi.omegiextension;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.lang.reflect.Method;
import java.util.Arrays;
import net.bytebuddy.asm.Advice.Local;
import net.bytebuddy.asm.Advice.OnMethodEnter;
import net.bytebuddy.asm.Advice.OnMethodExit;
import net.bytebuddy.asm.Advice.Origin;
import net.bytebuddy.asm.Advice.Thrown;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;

public class OmegiExtendedInstrumentation implements TypeInstrumentation {

	@Override
	public ElementMatcher<TypeDescription> typeMatcher() {
		return (ElementMatcher<TypeDescription>)ElementMatchers.isAnnotatedWith((ElementMatcher)ElementMatchers.named("org.springframework.stereotype.Service"))
			.or((ElementMatcher)ElementMatchers.isAnnotatedWith((ElementMatcher)ElementMatchers.named("org.springframework.stereotype.Component")))
			.or((ElementMatcher)ElementMatchers.isAnnotatedWith((ElementMatcher) ElementMatchers.named("org.springframework.stereotype.Repository")));
	}

	@Override
	public void transform(TypeTransformer typeTransformer) {
		typeTransformer.applyAdviceToMethod((ElementMatcher)ElementMatchers.isPublic().and((ElementMatcher)ElementMatchers.isMethod()), getClass().getName() + "$OmegiAdvice");
	}

	public static class OmegiAdvice {

		@OnMethodEnter(suppress = Throwable.class)
		public static Scope onEnter(@Origin Method method, @Local("otelSpan") Span span, @Local("otelScope") Scope scope) {
			Tracer tracer = GlobalOpenTelemetry.getTracer("omegi", "omegi:1.0.0");
			String methodName =
				method.getDeclaringClass().getSimpleName() + "." + method.getDeclaringClass().getSimpleName();
			span = tracer.spanBuilder(methodName).startSpan();
			span.setAttribute("method", method.getDeclaringClass().getName() + "." + method.getName());
			span.setAttribute("param", Arrays.toString(method.getParameters()));
			scope = span.makeCurrent();
			return scope;
		}

		@OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
		public static void onExit(@Local("otelSpan") Span span, @Local("otelScope") Scope scope,
			@Thrown Throwable throwable) {
			scope.close();
			if (throwable != null) {
				span.setStatus(StatusCode.ERROR, "Exception thrown in method");
				span.recordException(throwable);
			}
			span.end();
		}
	}

}
