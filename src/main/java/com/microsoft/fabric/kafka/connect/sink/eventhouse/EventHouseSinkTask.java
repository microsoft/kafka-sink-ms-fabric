package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.kafka.connect.sink.eventhouse.internal.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EventHouseSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHouseSinkTask.class);

    private final Set<TopicPartition> topicPartitions;
    private final Map<TopicPartition, EventStreamBatchingIngestor> topicPartitionToIngestorMap;
    private final EventHouseRecordWriter eventHouseRecordWriter;
    private Map<String, TopicToTableMapping> topicTableMap;
    private EventHouseSinkConfig config;

    public EventHouseSinkTask() {
        this.topicPartitions = new HashSet<>();
        this.eventHouseRecordWriter = new EventHouseRecordWriter();
        this.topicPartitionToIngestorMap = new HashMap<>();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new EventHouseSinkConfig(props);
        this.topicTableMap = Arrays.stream(this.config.getTopicToTableMapping())
                .collect(Collectors.toMap(TopicToTableMapping::getTopic, Function.identity()));
    }

    @Override
    public void put(@NotNull Collection<SinkRecord> records) {
        Map<TopicPartition, List<String>> groupedRecords = new HashMap<>();
        records.stream().filter(record -> record != null && this.topicTableMap.containsKey(record.topic())).
                forEach(record -> {
                    TopicToTableMapping topicToTableMapping = this.topicTableMap.get(record.topic());
                    IngestionProperties.DataFormat targetFormat = IngestionProperties.DataFormat.
                            valueOf(topicToTableMapping.getFormat().toLowerCase(Locale.ROOT));
                    try {
                        String targetMessage = this.eventHouseRecordWriter.write(record, targetFormat);
                        groupedRecords.computeIfAbsent(new TopicPartition(record.topic(), record.kafkaPartition()), k -> new ArrayList<>())
                                .add(targetMessage);
                    } catch (Exception e) {
                        LOGGER.error("Failed to write record: {}", record, e);
                        throw new ConnectException("Failed to write record during conversion", e);
                    }
                });
        groupedRecords.forEach((topicPartition, messages) -> {
            EventStreamBatchingIngestor ingestor = this.topicPartitionToIngestorMap.get(topicPartition);
            if (ingestor == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestor mapped for the topic: %s. " +
                        "please check your configuration.", topicPartition.topic()));
            }
            try {
                ingestor.write(messages);
            } catch (IOException e) {
                LOGGER.error("Error while adding records for ingestion:", e);
                throw new ConnectException(e);
            }
        });
    }

    @Override
    public void stop() {
        this.topicPartitionToIngestorMap.values().forEach(esbi -> {
            try {
                esbi.close();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        });
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        this.topicPartitionToIngestorMap.values().forEach(esbi -> {
            try {
                esbi.flush();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        });
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : this.topicPartitions) {
            if (writers.get(tp) == null) {
                throw new ConnectException("Topic Partition not configured properly. " +
                        "verify your `topics` and `kusto.tables.topics.mapping` configurations");
            }
            Long lastCommittedOffset = writers.get(tp).lastCommittedOffset;

            if (lastCommittedOffset != null) {
                long offset = lastCommittedOffset + 1L;
                LOGGER.debug("Forwarding to framework request to commit offset: {} for {} while the offset is {}", offset,
                        tp, offsets.get(tp));
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        }

        return offsetsToCommit;
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        this.topicPartitions.addAll(partitions);
        for (TopicPartition topicPartition : this.topicPartitions) {
            LOGGER.info("Open Kusto topic: '{}' with partition: '{}'", topicPartition.topic(), topicPartition.partition());
            TopicToTableMapping topicToTableMapping = this.topicTableMap.get(topicPartition.topic());
            if (topicToTableMapping == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped " +
                        "for the topic: %s. please check your configuration.", topicPartition.topic()));
            } else {
                this.topicPartitionToIngestorMap.
                        putIfAbsent(topicPartition, new EventStreamBatchingIngestor(this.config, topicToTableMapping));
            }
        }
    }
}
