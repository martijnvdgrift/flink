package org.apache.flink.metrics.stackdriver;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 *
 */
public class StackdriverReporter extends AbstractReporter implements Scheduled {

	private static final Logger LOG = LoggerFactory.getLogger(StackdriverReporter.class);
	private String projectId;
	private String location;
	private String metricPrefix;

	private StackDriverClient client;

	@Override
	public String filterCharacters(String input) {
		//todo: can we use this to filter illegal (stackdriver) characters?
		//todo: can we use this to filter the *.internal stuff out of it?
		return null;
	}

	@Override
	public void open(MetricConfig config) {
		this.projectId = config.getString(StackDriverClient.PROJECT_ID_KEY, "bigdata_evaulation");
		this.location = config.getString(StackDriverClient.LOCATION_KEY, "europe-west4-a");
		this.metricPrefix = config.getString(StackDriverClient.METRIC_PREFIX_KEY, "custom.googleapis.com/b.flink/");
		try {
			client = new StackDriverClient(projectId);
		} catch (IOException e) {
			LOG.error("Error while opening Stackdriver Reporter, {}", e.getMessage());
		}
	}

	@Override
	public void close() {
		client.close();
	}

	@Override
	public void report() {

		LOG.info("Gauges: {}", gauges.entrySet());
		LOG.info("Counters: {}", counters.entrySet());
		LOG.info("Histograms: {}", histograms.entrySet());
		LOG.info("Meters: {}", meters.entrySet());

		client.intervalToCurrent();

		try {
			for (Map.Entry<Gauge<?>, String> entry : gauges.entrySet()) {
				LOG.info("Gauge Value: {}", entry.getKey().getValue());
				System.out.println(entry.getKey().getValue());
				MetricInformation metricInformation = new MetricInformation(projectId, location, metricPrefix, entry.getValue());
				client.reportGauge(metricInformation, entry.getKey());
			}

			for (Map.Entry<Counter, String> entry : counters.entrySet()) {
				MetricInformation metricInformation = new MetricInformation(projectId, location, metricPrefix, entry.getValue());
				client.reportCounter(metricInformation, entry.getKey());
			}

			for (Map.Entry<Histogram, String> entry : histograms.entrySet()) {
				MetricInformation metricInformation = new MetricInformation(projectId, location, metricPrefix, entry.getValue());
				client.reportHistogram(metricInformation, entry.getKey());
			}

			for (Map.Entry<Meter, String> entry : meters.entrySet()) {
				MetricInformation metricInformation = new MetricInformation(projectId, location, metricPrefix, entry.getValue());
				client.reportMeter(metricInformation, entry.getKey());
			}
		} catch (ConcurrentModificationException | NoSuchElementException | NullPointerException e) {
			LOG.error("Error while looping over values: {}", e.getMessage());


		}
		//todo: uncomment this :)
		//		client.send();
	}
}
