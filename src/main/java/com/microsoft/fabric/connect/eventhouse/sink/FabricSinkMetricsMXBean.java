package com.microsoft.fabric.connect.eventhouse.sink;

/**
 * MXBean interface for Fabric Sink Connector JMX metrics.
 */
public interface FabricSinkMetricsMXBean {

    long getRecordsWritten();

    long getRecordsFailed();

    long getIngestionAttempts();

    long getIngestionSuccesses();

    long getIngestionFailures();

    long getDlqRecordsSent();
}
