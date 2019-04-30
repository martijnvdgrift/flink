package org.apache.flink.metrics.stackdriver;

import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Map;

import static org.apache.flink.metrics.stackdriver.MetricConstants.*;

public class MetricInformationTest {

	String testProjectId = "example-project";
	String testLocation = "europe-west4-a";
	String testMetricPrefix = "custom.googleapis.com/flink/";

	private Map<String, String> mapWithExpectedValues(
		String namespace,
		String jobName,
		String taskId
	) {
		return ImmutableMap.of(
			"project_id", testProjectId,
			"location", testLocation,
			"namespace", namespace,
			"job", jobName,
			"task_id", taskId
		);
	}

	@Test
	public void metricInformationValueJVMTest() {
		String expectedMetricType = "custom.googleapis.com/flink/GarbageCollector.PSMarkSweep.Count";
		String expectedJobName = "";
		String expectedTaskId = "JVM";
		String expectedNamespace = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-0.jobmanager.Status";

		Map<String, String> expectedMap = mapWithExpectedValues(expectedNamespace, expectedJobName, expectedTaskId);

		MetricInformation testMetricInformation = new MetricInformation(testProjectId, testLocation, testMetricPrefix, rawMetricValueJVMWithSpace);

		Assertions
			.assertThat(testMetricInformation.getMetricType())
			.isEqualTo(expectedMetricType);

		Assertions
			.assertThat(testMetricInformation.getResourceLabels())
			.isEqualTo(expectedMap);

	}

	@Test
	public void metricInformationJobMetrics(){
		String expectedMetricType = "custom.googleapis.com/flink/numRunningJobs";
		String expectedNamespace = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-2.jobmanager";
		String expectedTaskId = "";
		String expectedJobName = "";

		Map<String, String> expectedMap = mapWithExpectedValues(expectedNamespace, expectedJobName, expectedTaskId);
		MetricInformation testMetricInformation = new MetricInformation(testProjectId, testLocation, testMetricPrefix, numRunningJobsMetrics);

		Assertions
			.assertThat(testMetricInformation.getMetricType())
			.isEqualTo(expectedMetricType);

		Assertions
			.assertThat(testMetricInformation.getResourceLabels())
			.isEqualTo(expectedMap);
	}

	@Test
	public void metricInformationTaskManagerTest() {
		String expectedMetricType = "custom.googleapis.com/flink/KafkaConsumer.response-rate";
		String expectedJobName = "Flink_Streaming_:_Mysql_Ingestion_for_topic_mysqlCdc_bp";
		String expectedNamespace = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-3.taskmanager.container_1556118848505_0024_01_000002";
		String expectedTaskId = "KafkaSource-7";
		Map<String, String> expectedMap = mapWithExpectedValues(expectedNamespace, expectedJobName, expectedTaskId);

		MetricInformation testMetricInformation = new MetricInformation(testProjectId, testLocation, testMetricPrefix, rawMetricValueTaskManager1);

		System.out.println(testMetricInformation.getMetricType());
		Assertions
			.assertThat(testMetricInformation.getMetricType())
			.isEqualTo(expectedMetricType);

		Assertions
			.assertThat(testMetricInformation.getResourceLabels())
			.isEqualTo(expectedMap);
	}

	@Test
	public void metricInformationTaskManager2Test() {

		String expectedMetricType = "custom.googleapis.com/flink/buffers.inputQueueLength";
		String expectedJobName = "Flink_Streaming_:_Mysql_Ingestion_for_topic_mysqlCdc_bp";
		String expectedNamespace = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-0.taskmanager.container_1556118848505_0025_01_000002";
		String expectedTaskId = "KafkaSource-MessageStream-0";
		Map<String, String> expectedMap = mapWithExpectedValues(expectedNamespace, expectedJobName, expectedTaskId);


		MetricInformation testMetricInformation = new MetricInformation(testProjectId, testLocation, testMetricPrefix, rawMetricValueTaskManager3);

		Assertions
			.assertThat(testMetricInformation.getMetricType())
			.isEqualTo(expectedMetricType);

		Assertions
			.assertThat(testMetricInformation.getResourceLabels())
			.isEqualTo(expectedMap);
	}
}
