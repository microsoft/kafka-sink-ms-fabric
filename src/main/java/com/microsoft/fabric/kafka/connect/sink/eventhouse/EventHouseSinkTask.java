package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.kafka.connect.sink.Version;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Kusto sink uses file system to buffer records.
 * Every time a file is rolled, we used the kusto client to ingest it.
 * Currently only ingested files are "committed" in the sense that we can
 * advance the offset according to it.
 */
public class EventHouseSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(EventHouseSinkTask.class);
    private final Set<TopicPartition> assignment;
    protected IngestClient kustoIngestClient;
    protected IngestClient streamingIngestClient;
    protected Map<TopicPartition, TopicPartitionWriter> writers;
    private Map<String, TopicIngestionProperties> topicsToIngestionProps;
    private EventHouseSinkConfig config;
    private boolean isDlqEnabled;
    private String dlqTopicName;
    private Producer<byte[], byte[]> dlqProducer;

    public EventHouseSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
        // TODO we should check ingestor role differently
    }

    private static boolean isStreamingEnabled(@NotNull EventHouseSinkConfig config) throws JsonProcessingException {
        return Arrays.stream(config.getTopicToTableMapping()).anyMatch(TopicToTableMapping::isStreaming);
    }

    public static @NotNull ConnectionStringBuilder createKustoEngineConnectionString(@NotNull EventHouseSinkConfig config) {
        ConnectionStringBuilder kcsb = new ConnectionStringBuilder(config.getConnectionString());
        if (Objects.requireNonNull(config.getAuthStrategy()) == EventHouseSinkConfig.KustoAuthenticationStrategy.AZ_DEV_TOKEN) {
            log.warn("Using DEV-TEST mode, use this for development only. NOT recommended for production scenarios");
            kcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
                    kcsb.getClusterUrl(),
                    config.getAuthAccessToken());
        } else {
            throw new ConfigException("Failed to initialize KustoIngestClient, please " +
                    "provide valid credentials. Either Kusto managed identity or " +
                    "Kusto appId, appKey, and authority should be configured.");
        }
        kcsb.setConnectorDetails(Version.CLIENT_NAME, Version.getVersion(), null, null,
                false, null, Pair.emptyArray());
        return kcsb;
    }

    public static Map<String, TopicIngestionProperties> getTopicsToIngestionProps(EventHouseSinkConfig config) {
        Map<String, TopicIngestionProperties> result = new HashMap<>();

        try {
            TopicToTableMapping[] mappings = config.getTopicToTableMapping();
            for (TopicToTableMapping mapping : mappings) {
                IngestionProperties props = new IngestionProperties(mapping.getDb(), mapping.getTable());
                String format = mapping.getFormat();
                log.debug("Using format {} ", format);
                if (format != null && !format.isEmpty()) {
                    props.setDataFormat(format);
                }
                String mappingRef = mapping.getMapping();
                if (StringUtils.isNotEmpty(mappingRef) && StringUtils.isNotEmpty(format)) {
                    props.setIngestionMapping(mappingRef,
                            IngestionMapping.IngestionMappingKind.valueOf(format.toUpperCase(Locale.ROOT)));
                }
                TopicIngestionProperties topicIngestionProperties = new TopicIngestionProperties();
                topicIngestionProperties.ingestionProperties = props;
                topicIngestionProperties.streaming = mapping.isStreaming();
                result.put(mapping.getTopic(), topicIngestionProperties);
            }
            return result;
        } catch (Exception ex) {
            throw new ConfigException("Error while parsing kusto ingestion properties.", ex);
        }
    }

    public void createKustoIngestClient(EventHouseSinkConfig config) {
        try {
            HttpClientProperties httpClientProperties = null;
            if (StringUtils.isNotEmpty(config.getConnectionProxyHost()) && config.getConnectionProxyPort() > -1) {
                httpClientProperties = HttpClientProperties.builder()
                        .proxy(new HttpHost(config.getConnectionProxyHost(), config.getConnectionProxyPort())).build();
            }
            ConnectionStringBuilder ingestConnectionStringBuilder = createKustoEngineConnectionString(config);
            kustoIngestClient = httpClientProperties != null ? IngestClientFactory.createClient(ingestConnectionStringBuilder, httpClientProperties)
                    : IngestClientFactory.createClient(ingestConnectionStringBuilder);

            if (isStreamingEnabled(config)) {
                ConnectionStringBuilder streamingConnectionStringBuilder = createKustoEngineConnectionString(config);
                streamingIngestClient = httpClientProperties != null
                        ? IngestClientFactory.createManagedStreamingIngestClient(ingestConnectionStringBuilder, streamingConnectionStringBuilder,
                                httpClientProperties)
                        : IngestClientFactory.createManagedStreamingIngestClient(ingestConnectionStringBuilder, streamingConnectionStringBuilder);
            }
        } catch (Exception e) {
            throw new ConnectException("Failed to initialize KustoIngestClient", e);
        }
    }

    public TopicIngestionProperties getIngestionProps(String topic) {
        return topicsToIngestionProps.get(topic);
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        assignment.addAll(partitions);
        for (TopicPartition tp : assignment) {
            TopicIngestionProperties ingestionProps = getIngestionProps(tp.topic());
            log.debug("Open Kusto topic: '{}' with partition: '{}'", tp.topic(), tp.partition());
            if (ingestionProps == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped " +
                        "for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                IngestClient client = ingestionProps.streaming ? streamingIngestClient : kustoIngestClient;
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, client, ingestionProps, config, isDlqEnabled,
                        dlqTopicName, dlqProducer);
                writer.open();
                writers.put(tp, writer);
            }
        }
    }

    @Override
    public void close(@NotNull Collection<TopicPartition> partitions) {
        log.warn("Closing writers in KustoSinkTask");
        CountDownLatch countDownLatch = new CountDownLatch(partitions.size());
        // First stop so that no more ingestions trigger from timer flushes
        partitions.forEach((TopicPartition tp) -> writers.get(tp).stop());
        for (TopicPartition tp : partitions) {
            try {
                writers.get(tp).close();
                // TODO: if we still get duplicates from rebalance - consider keeping writers
                // aside - we might
                // just get the same topic partition again
                writers.remove(tp);
                assignment.remove(tp);
            } catch (ConnectException e) {
                log.error("Error closing topic partition for {}.", tp, e);
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    @Override
    public void start(Map<String, String> props) {
        config = new EventHouseSinkConfig(props);
        String url = config.getConnectionString();
        if (config.isDlqEnabled()) {
            isDlqEnabled = true;
            dlqTopicName = config.getDlqTopicName();
            Properties properties = config.getDlqProps();
            log.info("Initializing miscellaneous dead-letter queue producer with the following properties: {}",
                    properties.keySet());
            try {
                dlqProducer = new KafkaProducer<>(properties);
            } catch (Exception e) {
                throw new ConnectException("Failed to initialize producer for miscellaneous dead-letter queue", e);
            }
        } else {
            dlqProducer = null;
            isDlqEnabled = false;
            dlqTopicName = null;
        }
        topicsToIngestionProps = getTopicsToIngestionProps(config);
        // this should be read properly from settings
        createKustoIngestClient(config);
        log.info("Started KustoSinkTask with target cluster: ({}), source topics: ({})", url,
                topicsToIngestionProps.keySet());
        // Adding this check to make code testable
        if (context != null) {
            open(context.assignment());
        }
    }

    @Override
    public void stop() {
        log.warn("Stopping KustoSinkTask");
        // First stop so that no more ingestion trigger from timer flushes
        for (TopicPartitionWriter writer : writers.values()) {
            writer.stop();
        }

        for (TopicPartitionWriter writer : writers.values()) {
            writer.close();
        }
        try {
            if (kustoIngestClient != null) {
                kustoIngestClient.close();
            }
        } catch (IOException e) {
            log.error("Error closing kusto client", e);
        }
    }

    @Override
    public void put(@NotNull Collection<SinkRecord> records) {
        SinkRecord lastRecord = null;
        for (SinkRecord sinkRecord : records) {
            lastRecord = sinkRecord;
            TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
            TopicPartitionWriter writer = writers.get(tp);
            if (writer == null) {
                NotFoundException e = new NotFoundException(String.format("Received a record without " +
                        "a mapped writer for topic:partition(%s:%d), dropping record.", tp.topic(), tp.partition()));
                log.error("Error putting records: ", e);
                throw e;
            }
            if (sinkRecord.value() == null) {
                log.warn("Filtering null value (tombstone) records at offset {}, key {} and partition {} ",
                        sinkRecord.kafkaOffset(), sinkRecord.key(), sinkRecord.kafkaPartition());
            } else {
                writer.writeRecord(sinkRecord);
            }
        }
        if (lastRecord != null) {
            log.debug("Last record offset: {}", lastRecord.kafkaOffset());
        }
    }

    // This is a neat trick, since our rolling files commit whenever they like,
    // offsets may drift
    // from what kafka expects. So basically this is to re-sync topic-partition
    // offsets with our sink.
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : assignment) {
            if (writers.get(tp) == null) {
                throw new ConnectException("Topic Partition not configured properly. " +
                        "verify your `topics` and `kusto.tables.topics.mapping` configurations");
            }
            Long lastCommittedOffset = writers.get(tp).lastCommittedOffset;
            if (lastCommittedOffset != null) {
                long offset = lastCommittedOffset + 1L;
                log.debug("Forwarding to framework request to commit offset: {} for {} while the offset is {}", offset,
                        tp, offsets.get(tp));
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        }
        return offsetsToCommit;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // do nothing , rolling files can handle writing
    }
}
