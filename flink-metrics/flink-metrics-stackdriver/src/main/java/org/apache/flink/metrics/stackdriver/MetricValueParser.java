package org.apache.flink.metrics.stackdriver;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class MetricValueParser {

	private static final Logger LOG = LoggerFactory.getLogger(MetricValueParser.class);

	private MetricValueParser() {
	}

	static String removeIpAdress(String inputString) {
		String findIpRegex = "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}.";
		return inputString.replaceFirst(findIpRegex, "");
	}

	private static String removeInternal(String inputString) {
		String removeMetadataRegex = "(.c.)([a-zA-Z0-9\\s_\\-:]*)(.internal)";
		return inputString.replaceFirst(removeMetadataRegex, "");
	}

	static String getHostName(String inputString) {
		Pattern pattern = Pattern.compile("^[^.]*");
		String withoutInternal = removeInternal(inputString);

		Matcher matcher = pattern.matcher(withoutInternal);
		if (matcher.find()) {
			return matcher.group(0);
		}
		return "";
	}

	static String getTaskId(String inputString) {
		String withoutWhiteSpaces = inputString.replaceAll("\\s+", "");

		if (!inputString.contains("taskmanager.container_")) {
			String selectAllTillOperator = "^[^.]*.[^.]*.[^.]*.[^.]*.[^.]*.[^.]*.";
			String removeAllAfterPoint = "\\.(.*)";
			String withoutBeginning = withoutWhiteSpaces.replaceFirst(selectAllTillOperator, "");
			return withoutBeginning.replaceAll(removeAllAfterPoint, "");
		}


		String removeAllTillSource = "^[^.]*.[\\w]*.[\\w]*.[\\w:;]*.[\\w]*:";
		Pattern operatorSubtaskPattern = Pattern.compile("^[^.]*.\\d");
		String withoutIllegalCharacter = withoutWhiteSpaces.replace(">", "");
		String remainder = withoutIllegalCharacter.replaceFirst(removeAllTillSource, "");
		Matcher matcher = operatorSubtaskPattern.matcher(remainder);

		if (matcher.find()) {
			return matcher.group(0).replaceAll("\\.", "-");
		}
		return "";
	}

	static String getSanitizedJobName(String inputSting) {
		String hostNameAndManagerPattern = "^[^.]*.[\\w]*.[\\w]*.";
		Pattern jobNamePattern = Pattern.compile("^[^.]*");

		if (!inputSting.contains("taskmanager.container_")) {
			return "";
		}

		String hostNameAndManagerRemoved = inputSting.replaceFirst(hostNameAndManagerPattern, "");
		Matcher matcher = jobNamePattern.matcher(hostNameAndManagerRemoved);

		if (matcher.find()) {
			return matcher.group(0).replaceAll("\\s+", "_");
		}
		return "";
	}

	static String getSanitizedMetricName(String inputString) {
		String withoutWhiteSpaces = inputString.replaceAll("\\s+", "");
		if (inputString.toLowerCase().contains("taskmanager.container_")) {
			String withRemovedIllegalCharacter = withoutWhiteSpaces.replaceAll(">", "");
			String removeAllExpectLatestPattern = "^[^.]*.[^.]*.[^.]*.[^.]*.[^.]*.\\d.";
			return withRemovedIllegalCharacter.replaceAll(removeAllExpectLatestPattern, "");

		} else if (inputString.toLowerCase().contains(".jobmanager.status")) {
			String removeAllExceptLatestPattern = "^[^.]*.[^.]*.[^.]*.[^.]*.[^.]*.[^.]*.[^.]*.";
			return withoutWhiteSpaces.replaceAll(removeAllExceptLatestPattern, "");

		} else if (inputString.toLowerCase().contains(".jobmanager.")) {
			String removeAllExceptLatest = "^[^.]*.[^.]*.[^.]*.[^.]*.[^.]*.";
			return withoutWhiteSpaces.replaceAll(removeAllExceptLatest, "");

		} else {
			LOG.error("Couldn't parse metricname for inputstring : {}", inputString);
			return "";
		}
	}

	static String getTaskManager(String inputString) {
		String removeHostNamePattern = "^[^.]*.";
		String withoutInternal = removeInternal(inputString);
		String withoutHostname = withoutInternal.replaceFirst(removeHostNamePattern, "");

		if (inputString.toLowerCase().contains(".taskmanager.container_") || inputString.toLowerCase().contains(".jobmanager.status.")) {
			Pattern pattern = Pattern.compile("^[^.]*.[\\w]*");
			Matcher matcher = pattern.matcher(withoutHostname);
			if (matcher.find()) {
				return matcher.group(0);
			}
		} else if (inputString.toLowerCase().contains(".jobmanager.")) {
			return "jobmanager";
		}

		return "";
	}


}
