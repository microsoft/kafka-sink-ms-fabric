package com.microsoft.fabric.kafka.connect.sink.es;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.fabric.kafka.connect.sink.Version;
import com.microsoft.fabric.kafka.connect.sink.formatwriter.HeaderAndMetadataWriter;

public class EventStreamSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamSinkTask.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private HeaderAndMetadataWriter headerAndMetadataWriter;
    private EventStreamSinkConfig config;
    private Producer<String, String> esProducer;

    @Override
    public String version() {
        return Version.getVersion();
    }


    @Override
    public void open(Collection<TopicPartition> partitions) {
        super.open(partitions);
    }

    @Override
    public void start(Map<String, String> connectorConfig) {
        headerAndMetadataWriter = new HeaderAndMetadataWriter();
        config = new EventStreamSinkConfig(connectorConfig);
        esProducer = new KafkaProducer<>(config.getEsProducerProperties());
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        try {
            publishEvents(sinkRecords);
        } catch (IOException e) {
            LOGGER.error("Failed to publish events to EventHub, this will be thrown as a runtime exception", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {

    }
    private void publishEvents(@NotNull Collection<SinkRecord> sinkRecords) throws IOException {
        for(SinkRecord record: sinkRecords) {
            Map<String,Object> parsedKeys =
                    headerAndMetadataWriter.convertSinkRecordToMap(record, true,
                            config.getTargetDataFormat()).stream().reduce(new HashMap<>(),
                            (acc, map) -> {
                                acc.putAll(map);
                                return acc;
                            });
            Collection<Map<String, Object>> parsedValues = headerAndMetadataWriter.convertSinkRecordToMap(record, false, config.getTargetDataFormat());
            String recordKey = (record.key() == null) ? "" : OBJECT_MAPPER.writeValueAsString(parsedKeys);
            parsedValues.forEach(parsedValue -> {
                try {
                    String recordValue = (record.value() == null) ? "" : OBJECT_MAPPER.writeValueAsString(parsedValue);
                    final ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(config.getEventHubName(), recordKey,recordValue);
                    esProducer.send(producerRecord);
                } catch (JsonProcessingException e) {
                    LOGGER.error("Failed to parse record to JSON", e);
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
        LOGGER.debug("Flushing...");
        // Process results of all the outstanding futures specified by each TopicPartition.
        for (Map.Entry<TopicPartition, OffsetAndMetadata> partitionOffset :
                partitionOffsets.entrySet()) {
            LOGGER.trace("Received flush for partition {} " , partitionOffset.getKey().toString());
        }
    }
}
