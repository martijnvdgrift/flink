package org.apache.flink.metrics.stackdriver;

import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.metrics.stackdriver.MetricConstants.*;

public class MetricValueParserTest {

	//todo: test if the parser works with spaces.
	//todo: also implement it in the characterfilter, since it's screwing with stackdrivers accepted names.

	@Test
	public void metricValueParserRemoveIpTest() {
		String ip = "127.0.0.1";
		String metricWithoutIp = MetricValueParser.removeIpAdress(rawMetricValueWithIp);
		assert !metricWithoutIp.contains(ip);
	}

	@Test
	public void metricValueGetHostName() {
		String expectedHostName1 = "bigdata-03325d6f-1d86-4c12-bf06-a7a14bbea58a-w-2";
		String parsedHostName1 = MetricValueParser.getHostName(rawMetricValueWithHostname);
		Assert.assertThat(expectedHostName1, CoreMatchers.is(parsedHostName1));

		String expectedHostName2 = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-0";
		String parsedHostName2 = MetricValueParser.getHostName(rawMetricValueWithJVM);
		Assert.assertThat(expectedHostName2, CoreMatchers.is(parsedHostName2));
	}

	@Test
	public void getNamespaceTest() {
		String expectedNamespace1 = "taskmanager.container_1556118848505_0024_01_000002";
		String parsedNamespace1 = MetricValueParser.getTaskManager(rawMetricValueTaskManager1);


		String expectedNamespace2 = "jobmanager.Status";
		String parsedNamespace2 = MetricValueParser.getTaskManager(rawMetricValueJVMWithSpace);

		Assertions
			.assertThat(expectedNamespace1)
			.isEqualTo(parsedNamespace1);

		Assertions
			.assertThat(expectedNamespace2)
			.isEqualTo(parsedNamespace2);
	}

	@Test
	public void jobNameTest() {
		String expectedJobName1 = "Flink_Streaming_:_Mysql_Ingestion_for_topic_mysqlCdc_bp";
		String parsedJobName1 = MetricValueParser.getSanitizedJobName(rawMetricValueTaskManager1);

		Assertions.assertThat(parsedJobName1)
			.isEqualTo(expectedJobName1);

		String expectedJobName2 = "";
		String parsedJobName2 = MetricValueParser.getSanitizedJobName(rawMetricValueJVMWithSpace);

		Assertions.assertThat(parsedJobName2)
			.isEqualTo(expectedJobName2);
	}

	@Test
	public void taskIdTest() {
		String expectedTaskId1 = "KafkaSource-7";
		String parsedTaskId1 = MetricValueParser.getTaskId(rawMetricValueTaskManager1);

		Assertions.assertThat(expectedTaskId1)
			.isEqualTo(parsedTaskId1);

		String expectedTaskId2 = "";
		String parsedTaskId2 = MetricValueParser.getTaskId(rawMetricValueWithJVM);

		Assertions.assertThat(expectedTaskId2)
			.isEqualTo(parsedTaskId2);

		String expectedTaskId3 = "";
		String parsedTaskId3 = MetricValueParser.getTaskId(rawMetricValueJVMWithSpace);

		Assertions.assertThat(expectedTaskId3)
			.isEqualTo(parsedTaskId3);

		String expectedTaskId4 = "KafkaSource-3";
		String parsedTaskId4 = MetricValueParser.getTaskId(rawMetricValueTaskManager2);

		Assertions.assertThat(expectedTaskId4)
			.isEqualTo(parsedTaskId4);
	}

	@Test
	public void metricNameTest() {
		String expectedMetricName1 = "KafkaConsumer.response-rate";
		String parsedMetricName1 = MetricValueParser.getSanitizedMetricName(rawMetricValueTaskManager1);

		Assertions.assertThat(parsedMetricName1)
			.isEqualTo(expectedMetricName1);

		String expectedMetricName2 = "Memory.Direct.Count";
		String parsedMetricName2 = MetricValueParser.getSanitizedMetricName(rawMetricValueWithJVM);

		Assertions.assertThat(parsedMetricName2)
			.isEqualTo(expectedMetricName2);

		String expectedMetricName3 = "GarbageCollector.PSMarkSweep.Count";
		String parsedMetricName3 = MetricValueParser.getSanitizedMetricName(rawMetricValueJVMWithSpace);

		Assertions.assertThat(parsedMetricName3)
			.isEqualTo(expectedMetricName3);

		String expectedMetricName4 = "select-rate";
		String parsedMetricName4 = MetricValueParser.getSanitizedMetricName(rawMetricValueTaskManager2);

		Assertions.assertThat(parsedMetricName4)
			.isEqualTo(expectedMetricName4);
	}
}
