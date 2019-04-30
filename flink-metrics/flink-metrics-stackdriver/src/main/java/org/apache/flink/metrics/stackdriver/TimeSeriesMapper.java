package org.apache.flink.metrics.stackdriver;

import com.google.api.Distribution;
import com.google.api.Metric;
import com.google.api.MetricDescriptor;
import com.google.api.MonitoredResource;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import org.apache.flink.metrics.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Maps Flink metrics to StackDriver timeseries
 */
public class TimeSeriesMapper {

	//todo:
	static Collection<TimeSeries> map(MetricInformation metricInfo, TimeInterval interval, Histogram histogram) {
//		return createTimeSeries(metricInfo, createPoint(interval, createValue(histogram)));
		return new ArrayList<>();
	}

	static Collection<TimeSeries> map(MetricInformation metricInfo, TimeInterval interval, Meter meter) {
		return Arrays.asList(
			createTimeSeries(metricInfo, "/count", createPoint(interval, createCountValue(meter))),
			createTimeSeries(metricInfo, "/rate", createPoint(interval, createRateValue(meter)))
		);
	}


	static TimeSeries map(MetricInformation metricInfo, TimeInterval interval, Counter counter) {
		return createTimeSeries(metricInfo, createPoint(interval, createValue(counter)));
	}

	static TimeSeries map(MetricInformation metricInfo, TimeInterval interval, Gauge<?> gauge) {
		return createTimeSeries(metricInfo, createPoint(interval, createValue(gauge)));
	}

//	private static TypedValue createValue(Histogram histogram) {
//		//todo:
////		return TypedValue.newBuilder()
////			.setDistributionValue()
////			.setDistributionValue()
////			.build();
//		return null;
//	}

	private static TypedValue createCountValue(Meter meter) {

		return TypedValue.newBuilder()
			.setInt64Value(meter.getCount())
			.build();
	}

	private static TypedValue createRateValue(Meter meter) {
		return TypedValue.newBuilder()
			.setDoubleValue(meter.getRate())
			.build();
	}

	private static TypedValue createValue(Counter counter) {
		return TypedValue.newBuilder()
			.setInt64Value(counter.getCount())
			.build();
	}

	private static TypedValue createValue(Gauge<?> gauge) {
		TypedValue.Builder valueBuilder = TypedValue.newBuilder();

		Object value = gauge.getValue();
		if (value instanceof Double) {
			valueBuilder.setDoubleValue((Double) value);
		} else if (value instanceof Number) {
			long longValue = ((Number) value).longValue();
			valueBuilder.setInt64Value(longValue);
		} else {
			valueBuilder.setStringValue(String.valueOf(value));
		}
		return valueBuilder.build();
	}

	private static Point createPoint(TimeInterval interval, TypedValue value) {
		return Point.newBuilder()
			.setInterval(interval)
			.setValue(value)
			.build();
	}

	private static MonitoredResource createResource(MetricInformation metricInfo) {
		return MonitoredResource.newBuilder()
			.setType("generic_task")
			.putAllLabels(metricInfo.getResourceLabels())
			.build();
	}

	private static Metric createMetric(MetricInformation metricInfo, String suffix) {
		// todo: do we need to set labels here?
		//todo: do something with parsing it into the correct name

		return Metric.newBuilder()
			.setType(metricInfo.getMetricType() + suffix)
			.build();
	}

	private static TimeSeries createTimeSeries(MetricInformation metricInfo, Point point) {
		return TimeSeries.newBuilder()
			.setMetric(createMetric(metricInfo, ""))
			.setResource(createResource(metricInfo))
			.setMetricKind(MetricDescriptor.MetricKind.GAUGE)
			.addPoints(point)
			.build();
	}

	private static TimeSeries createTimeSeries(MetricInformation metricInfo, String suffix, Point point) {
		return TimeSeries.newBuilder()
			.setMetric(createMetric(metricInfo, suffix))
			.setResource(createResource(metricInfo))
			.setMetricKind(MetricDescriptor.MetricKind.GAUGE)
			.addPoints(point)
			.build();
	}

//	//todo: not used.
//	private static Distribution createDistribution(Histogram histogram) {
//		HistogramStatistics stats = histogram.getStatistics();
//
//		return Distribution.newBuilder()
//			.setMean(stats.getMean())
//			.setCount(histogram.getCount())
//			.build();
//	}
}
