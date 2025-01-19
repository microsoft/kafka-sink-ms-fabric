package com.microsoft.fabric.connect.eventhouse.sink.dlq;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Interface for reporting errors that occur while processing a record. Handle cases where old versions of KafkaConnect
 * are being used and the ErrantRecordReporter interface is not available.
 */
public interface KafkaRecordErrorReporter {
    void reportError(SinkRecord sinkRecord, Exception e);
}
