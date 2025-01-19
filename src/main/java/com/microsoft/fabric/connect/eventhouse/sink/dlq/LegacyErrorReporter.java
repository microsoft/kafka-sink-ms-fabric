package com.microsoft.fabric.connect.eventhouse.sink.dlq;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConfig;

public class LegacyErrorReporter implements KafkaRecordErrorReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(LegacyErrorReporter.class);
    private final Producer<byte[], byte[]> dlqProducer;
    private final String dlqTopicName;

    public LegacyErrorReporter(@NotNull FabricSinkConfig config) {
        Properties properties = config.getDlqProps();
        LOGGER.info("Initializing miscellaneous dead-letter queue producer with the following properties: {}",
                properties);
        try {
            this.dlqTopicName = config.getDlqTopicName();
            this.dlqProducer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            throw new ConnectException("Failed to initialize producer for miscellaneous dead-letter queue", e);
        }
    }

    @Override
    public void reportError(SinkRecord sinkRecord, Exception e) {
        this.sendFailedRecordToDlq(sinkRecord);
    }

    private void sendFailedRecordToDlq(@NotNull SinkRecord sinkRecord) {
        String contextMessage = String.format("Sending record to DLQ for the record with the following metadata, "
                + "topic=%s, partition=%s, offset=%s.",
                sinkRecord.topic(),
                sinkRecord.kafkaPartition(),
                sinkRecord.kafkaOffset());
        LOGGER.warn(contextMessage);
        ProducerRecord<byte[], byte[]> dlqRecord = dlqRecordWithHeaders(sinkRecord);
        if (dlqRecord == null) {
            LOGGER.warn("Failed to send record to DLQ. Topic: {}, Partition: {}, Offset: {}",
                    sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
            return;
        }
        dlqProducer.send(dlqRecord, (recordMetadata, exception) -> {
            if (recordMetadata != null) {
                LOGGER.info("Record sent to DLQ with metadata, topic={}, partition={}, offset={}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            }
            if (exception != null) {
                throw new KafkaException(
                        String.format("Failed to write records to miscellaneous dead-letter queue topic=%s.", dlqTopicName),
                        exception);
            } else {
                if (recordMetadata != null) {
                    LOGGER.debug("Record sent to DLQ with metadata, topic={}, partition={}, offset={}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                }
            }
        });
    }

    private @Nullable ProducerRecord<byte[], byte[]> dlqRecordWithHeaders(@NotNull SinkRecord sinkRecord) {
        byte[] valueRecord = null;
        byte[] keyRecord = null;
        if (sinkRecord.value() != null) {
            valueRecord = Values.convertToString(sinkRecord.valueSchema(), sinkRecord.value()).getBytes(StandardCharsets.UTF_8);
        }
        if (sinkRecord.key() != null) {
            keyRecord = Values.convertToString(sinkRecord.keySchema(), sinkRecord.key()).getBytes(StandardCharsets.UTF_8);
        }
        if (keyRecord != null) {
            ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(dlqTopicName, keyRecord, valueRecord);
            // Restore existing headers
            sinkRecord.headers().forEach(header -> {
                if (header.value() != null) {
                    String headerValue = Values.convertToString(header.schema(), header.value());
                    if (StringUtils.isNotEmpty(headerValue)) {
                        dlqRecord.headers().add(header.key(), headerValue.getBytes(StandardCharsets.UTF_8));
                    }
                }
            });
            dlqRecord.headers().add("kafka_topic", sinkRecord.topic().getBytes(StandardCharsets.UTF_8));
            dlqRecord.headers().add("kafka_partition", String.valueOf(sinkRecord.kafkaPartition()).getBytes(StandardCharsets.UTF_8));
            dlqRecord.headers().add("kafka_offset", String.valueOf(sinkRecord.kafkaOffset()).getBytes(StandardCharsets.UTF_8));
            dlqRecord.headers().add("kafka_timestamp", String.valueOf(sinkRecord.timestamp()).getBytes(StandardCharsets.UTF_8));
            return dlqRecord;
        }
        return null;
    }
}
