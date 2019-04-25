package org.apache.flink.metrics.stackdriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MetricInformation {
	private static final Logger LOG = LoggerFactory.getLogger(MetricInformation.class);


	private String metricPrefix;
	private String projectId;
	private String location;

	private String metricValue;
	private String metricType;

	private String namespace;
	private String jobName;
	private String instanceName;


	//todo: add constructor based on the string name and parse that into the correct stuff!


	public MetricInformation(String projectId, String location, String metricPrefix, String metricValue) {
		this.projectId = projectId;
		this.location = location;
		this.metricPrefix = metricPrefix;

		parseMetricValue(metricValue);
	}

	public static void parseMetricValue(String metricValue) {
		LOG.info("Testing - Seen metricvalue: {}", metricValue);
		//todo: parse JobName, task_id, namespace & metricType from this metricValue
		System.out.println(metricValue);


	}

	public String getMetricType(){
		return this.metricType;
	}

	public String fullyQualifiedMetricName() {
		return metricPrefix + metricValue;
	}

	//todo: check if this matches with what we actually want and what makes sense.
	//todo: https://cloud.google.com/monitoring/api/resources#tag_generic_task
	public Map<String, String> getResourceLabels() {
		Map<String, String> labelsMap = new HashMap<>();
		labelsMap.put("project_id", this.projectId);
		labelsMap.put("location", this.location);
		labelsMap.put("namespace", this.namespace);
		labelsMap.put("job_name", this.jobName);
		labelsMap.put("instance_name", this.instanceName);
		return labelsMap;

	}

}
