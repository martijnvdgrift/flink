package org.apache.flink.metrics.stackdriver;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class MetricInformationTest {
	private final String rawMetricValueWithIp = "127.0.0.1.taskmanager.ABCDE.MyJob.MyOperator.1.numRecordsIn";
	private final String rawMetricValueWithhostname = "bigdata-03325d6f-1d86-4c12-bf06-a7a14bbea58a-w-2.c.bigdata-ingest-mysql.internal.taskmanager.ABCDE.MyJob.MyOperator.1.numRecordsIn";


	@Test
	public void metricValueParserRemoveIpTest() {
		String ip = "127.0.0.1";
		String metricWithoutIp = MetricValueParser.removeIpAdress(rawMetricValueWithIp);
		assert !metricWithoutIp.contains(ip);

	}

	@Test
	public void metricValueGetHostName() {
		String expectedHostName = "bigdata-03325d6f-1d86-4c12-bf06-a7a14bbea58a-w-2";
		String parsedHostName = MetricValueParser.getHostName(rawMetricValueWithhostname);
		Assert.assertThat(expectedHostName, CoreMatchers.is(parsedHostName));
	}

	@Test
	public void metricValueParserRemovesHostname() {
		MetricValueParser.getHostName(rawMetricValueWithhostname);

	}
}
