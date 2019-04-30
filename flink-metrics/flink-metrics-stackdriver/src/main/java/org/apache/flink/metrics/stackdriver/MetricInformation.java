package org.apache.flink.metrics.stackdriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class MetricInformation {
	private static final Logger LOG = LoggerFactory.getLogger(MetricInformation.class);

	private String metricPrefix;
	private String projectId;
	private String location;

	private String metricName;

	private String namespace;
	private String jobName;
	private String taskId;


	MetricInformation(String projectId, String location, String metricPrefix, String metricValue) {
		this.projectId = projectId;
		this.location = location;
		this.metricPrefix = metricPrefix;

		parseMetricValue(metricValue);
	}

	private void parseMetricValue(String metricValue) {
		this.namespace = getNamespace(metricValue);
		this.jobName = MetricValueParser.getSanitizedJobName(metricValue);
		this.taskId = MetricValueParser.getTaskId(metricValue);
		this.metricName = MetricValueParser.getSanitizedMetricName(metricValue);
	}

	private String getNamespace(String metricValue) {
		return MetricValueParser.getHostName(metricValue) +
			"." +
			MetricValueParser.getTaskManager(metricValue);
	}


	String getMetricType() {
		return this.metricPrefix + this.metricName;
	}


	Map<String, String> getResourceLabels() {
		Map<String, String> labelsMap = new HashMap<>();
		labelsMap.put("project_id", this.projectId);
		labelsMap.put("location", this.location);
		labelsMap.put("namespace", this.namespace); //clustername + taskmanager id
		labelsMap.put("job", this.jobName);
		labelsMap.put("task_id", this.taskId); //operator + subtask
		return labelsMap;
	}

}
