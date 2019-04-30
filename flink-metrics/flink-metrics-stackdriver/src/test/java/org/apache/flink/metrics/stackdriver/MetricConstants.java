package org.apache.flink.metrics.stackdriver;

public interface MetricConstants {
	String rawMetricValueWithIp = "127.0.0.1.taskmanager.ABCDE.MyJob.MyOperator.1.numRecordsIn";
	String rawMetricValueWithHostname = "bigdata-03325d6f-1d86-4c12-bf06-a7a14bbea58a-w-2.c.bigdata-ingest-mysql.internal.taskmanager.ABCDE.MyJob.MyOperator.1.numRecordsIn";
	String rawMetricValueWithJVM = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-0.c.bigdata-evaluation.internal.jobmanager.Status.JVM.Memory.Direct.Count";
	String rawMetricValueTaskManager1 = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-3.taskmanager.container_1556118848505_0024_01_000002.Flink Streaming : Mysql Ingestion for topic mysqlCdc_bp.Source: KafkaSource.7.KafkaConsumer.response-rate";
	String rawMetricValueTaskManager2 = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-0.taskmanager.container_1556118848505_0025_01_000002.Flink Streaming : Mysql Ingestion for topic mysqlCdc_bp.Source: KafkaSource.3.select-rate";
	String rawMetricValueTaskManager3 = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-0.taskmanager.container_1556118848505_0025_01_000002.Flink Streaming : Mysql Ingestion for topic mysqlCdc_bp.Source: KafkaSource->MessageStream.0.buffers.inputQueueLength";

	String rawMetricValueJVMWithSpace = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-0.c.bigdata-evaluation.internal.jobmanager.Status.JVM.GarbageCollector.PS MarkSweep.Count";
	String numRunningJobsMetrics = "bigdata-21007439-91f2-4cf4-b4b8-4acbc267729c-w-2.c.bigdata-evaluation.internal.jobmanager.numRunningJobs";
}
