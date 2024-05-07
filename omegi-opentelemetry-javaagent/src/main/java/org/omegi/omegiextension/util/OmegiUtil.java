package org.omegi.omegiextension.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OmegiUtil {

	private static final String OMEGI_TOKEN_PROPERTY = "otel.exporter.omegi.token";
	private static final String SERVICE_NAME = "otel.service.name";

	public static String getToken() {
		String omegiToken = System.getProperty(OMEGI_TOKEN_PROPERTY);
		if (omegiToken == null || omegiToken.isEmpty()) {
			return "please set otel.exporter.omegi.token";
		} else {
			return omegiToken;
		}
	}

	public static String getServiceName() {
		String serviceName = System.getProperty(SERVICE_NAME);
		if (serviceName == null || serviceName.isEmpty()) {
			return "please set otel.service.name";
		} else {
			return serviceName;
		}
	}

	public static String getFormattedTime(long time) {
		Instant instant = Instant.ofEpochSecond(time / 1_000_000_000L, time % 1_000_000_000L);
		ZoneId zoneId = ZoneId.of("Asia/Seoul");
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
			.withZone(zoneId);
		return formatter.format(instant);
	}

	public static List<String> analyzeErrorFlow(String errorMessage) {
		List<String> errorFlow = new ArrayList<>();
		Pattern pattern = Pattern.compile("at (.*?)\\.([^.]*?)\\.([^.]*?)\\(");
		Matcher matcher = pattern.matcher(errorMessage);

		while (matcher.find()) {
			String packageName = matcher.group(1);
			String className = matcher.group(2);
			String methodName = matcher.group(3);

			String step = packageName + "." + className + "." + methodName;
			errorFlow.add(step);
		}

		return errorFlow;
	}
}
