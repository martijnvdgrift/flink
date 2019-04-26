package org.apache.flink.metrics.stackdriver;

import java.util.Arrays;

public class MetricValueParser {

	private MetricValueParser() {}

	static String removeIpAdress(String inputString) {
		String findIpRegex = "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}.";
		return inputString.replaceFirst(findIpRegex, "");
	}

	static String[] splitOnInternal(String inputString) {
		String removemetadataRegex = "(.c.)([a-zA-Z0-9\\s_\\-:]*)(.internal.)";

		return inputString.split(removemetadataRegex);
	}

	static String getHostName(String inputString) {
		return splitOnInternal(inputString)[0];
	}


}
