package com.microsoft.fabric.connect.eventhouse.sink.dlq;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpLoggerErrorReporter implements KafkaRecordErrorReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpLoggerErrorReporter.class);

    @Override
    public void reportError(SinkRecord sinkRecord, Exception e) {
        if (sinkRecord != null) {
            LOGGER.error("Failed to process record and no DLQ was configured : {}", sinkRecord, e);
        } else {
            LOGGER.error("Failed to process record and no DLQ was configured. SinkRecord was null", e);
        }
    }
}
