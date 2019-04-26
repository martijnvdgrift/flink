package org.apache.flink.metrics.stackdriver;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.util.Timestamps;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;

class StackDriverClient {

	static final String METRIC_PREFIX_KEY = "metricPrefix";
	static final String PROJECT_ID_KEY = "projectId";
	static final String LOCATION_KEY = "location";

	private String projectId;
	private Collection<TimeSeries> timeSeriesList;
	private TimeInterval interval;
	private MetricServiceClient serviceClient;

	void intervalToCurrent() {
		this.interval = TimeInterval.newBuilder()
			.setEndTime(Timestamps.fromNanos(Instant.now().getNano()))
			.build();
	}

	StackDriverClient(String projectId) throws IOException {
		this.projectId = projectId;
		this.timeSeriesList = new ArrayList<>();
		this.serviceClient = MetricServiceClient.create();
	}

	void reportGauge(MetricInformation info, Gauge<?> gauge) {
		if (this.interval == null) {
			intervalToCurrent();
		}
		timeSeriesList.add(TimeSeriesMapper.map(info, interval, gauge));
	}

	void reportCounter(MetricInformation info, Counter counter) {
		if (this.interval == null) {
			intervalToCurrent();
		}
		timeSeriesList.add(TimeSeriesMapper.map(info, interval, counter));
	}

	void reportMeter(MetricInformation info, Meter meter) {
		if (this.interval == null) {
			intervalToCurrent();
		}
		timeSeriesList.addAll(TimeSeriesMapper.map(info, interval, meter));
	}

	void reportHistogram(MetricInformation info, Histogram histogram) {
		if (this.interval == null) {
			intervalToCurrent();
		}
		timeSeriesList.addAll(TimeSeriesMapper.map(info, interval, histogram));
	}

	void send() {
		CreateTimeSeriesRequest request = CreateTimeSeriesRequest.newBuilder()
			.setName("projects/" + this.projectId)
			.addAllTimeSeries(timeSeriesList)
			.build();

		serviceClient.createTimeSeries(request);
	}

	void close() {
		serviceClient.close();
	}

}
