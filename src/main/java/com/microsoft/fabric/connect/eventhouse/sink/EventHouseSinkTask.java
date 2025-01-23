package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.ProxyOptions;
import com.azure.identity.WorkloadIdentityCredential;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.connect.eventhouse.sink.dlq.KafkaRecordErrorReporter;
import com.microsoft.fabric.connect.eventhouse.sink.dlq.LegacyErrorReporter;
import com.microsoft.fabric.connect.eventhouse.sink.dlq.NoOpLoggerErrorReporter;

/**
 * Kusto sink uses file system to buffer records.
 * Every time a file is rolled, we used the kusto client to ingest it.
 * Currently only ingested files are "committed" in the sense that we can
 * advance the offset according to it.
 */
public class EventHouseSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHouseSinkTask.class);
    private final Set<TopicPartition> assignment;
    protected IngestClient kustoIngestClient;
    protected IngestClient streamingIngestClient;
    protected Map<TopicPartition, TopicPartitionWriter> writers;
    private Map<String, TopicIngestionProperties> topicsToIngestionProps;
    private HeaderTransforms headerTransforms;
    private FabricSinkConfig config;
    private boolean isDlqEnabled;

    public EventHouseSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
    }

    private static boolean isStreamingEnabled(@NotNull FabricSinkConfig config) throws JsonProcessingException {
        return Arrays.stream(config.getTopicToTableMapping()).anyMatch(TopicToTableMapping::isStreaming);
    }

    public static @NotNull ConnectionStringBuilder createKustoEngineConnectionString(@NotNull final FabricSinkConfig config, final String clusterUrl) {
        final ConnectionStringBuilder connectionStringBuilder;
        switch (config.getAuthStrategy()) {
            case APPLICATION:
                if (StringUtils.isNotEmpty(config.getAuthAppId()) && StringUtils.isNotEmpty(config.getAuthAppKey())) {
                    connectionStringBuilder = ConnectionStringBuilder.createWithAadApplicationCredentials(
                            clusterUrl,
                            config.getAuthAppId(),
                            config.getAuthAppKey(),
                            config.getAuthAuthority());
                    // This is a special case as the APP ID is part of the ConnectionString.
                    // For all other cases, the auth has to be done with the cluster url and then
                    // adding the auth from the code specifically
                } else if (StringUtils.isNotEmpty(config.getConnectionString())) {
                    connectionStringBuilder = new ConnectionStringBuilder(config.getConnectionString());
                } else {
                    throw new ConfigException("Kusto authentication missing App Key.");
                }
                break;
            case MANAGED_IDENTITY:
                connectionStringBuilder = ConnectionStringBuilder.createWithAadManagedIdentity(
                        clusterUrl,
                        config.getAuthAppId());
                break;
            case WORKLOAD_IDENTITY:
                connectionStringBuilder = ConnectionStringBuilder.createWithAadTokenProviderAuthentication(
                        clusterUrl,
                        () -> {
                            WorkloadIdentityCredential wic = new WorkloadIdentityCredentialBuilder().build();
                            TokenRequestContext requestContext = new TokenRequestContext();
                            String clusterScope = String.format("%s/.default", clusterUrl);
                            requestContext.setScopes(Collections.singletonList(clusterScope));
                            AccessToken accessToken = wic.getTokenSync(requestContext);
                            if (accessToken != null) {
                                LOGGER.debug("Returned access token that expires at {}", accessToken.getExpiresAt());
                                return accessToken.getToken();
                            } else {
                                LOGGER.error("Obtained empty token during token refresh. Context {}", clusterScope);
                                throw new ConnectException("Failed to retrieve WIF token");
                            }
                        });
                break;
            case AZ_DEV_TOKEN:
                LOGGER.warn("Using DEV-TEST mode, use this for development only. NOT recommended for production scenarios");
                connectionStringBuilder = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
                        clusterUrl,
                        config.getAuthAccessToken());
                break;
            default:
                throw new ConfigException("Failed to initialize KustoIngestClient, please " +
                        "provide valid credentials. Either Kusto managed identity or " +
                        "Kusto appId, appKey, and authority should be configured.");
        }

        connectionStringBuilder.setConnectorDetails(Version.CLIENT_NAME, Version.getConnectorVersion(), Version.APP_NAME,
                Version.getConnectorVersion(), false, null, Pair.emptyArray());
        return connectionStringBuilder;
    }

    public static Map<String, TopicIngestionProperties> getTopicsToIngestionProps(FabricSinkConfig config) {
        Map<String, TopicIngestionProperties> result = new HashMap<>();

        try {
            TopicToTableMapping[] mappings = config.getTopicToTableMapping();
            for (TopicToTableMapping mapping : mappings) {
                IngestionProperties props = new IngestionProperties(mapping.getDb(), mapping.getTable());

                String format = mapping.getFormat();
                if (StringUtils.isNotEmpty(format)) {
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

    public void createKustoIngestClient(FabricSinkConfig config) {
        try {
            HttpClientProperties httpClientProperties = null;
            if (StringUtils.isNotEmpty(config.getConnectionProxyHost()) && config.getConnectionProxyPort() > -1) {
                httpClientProperties = HttpClientProperties.builder().proxy(new ProxyOptions(ProxyOptions.Type.HTTP,
                        new InetSocketAddress(config.getConnectionProxyHost(), config.getConnectionProxyPort()))).build();
            }
            ConnectionStringBuilder ingestConnectionStringBuilder = createKustoEngineConnectionString(config, config.getKustoIngestUrl());
            kustoIngestClient = httpClientProperties != null ? IngestClientFactory.createClient(ingestConnectionStringBuilder, httpClientProperties)
                    : IngestClientFactory.createClient(ingestConnectionStringBuilder);

            if (isStreamingEnabled(config)) {
                ConnectionStringBuilder streamingConnectionStringBuilder = createKustoEngineConnectionString(config, config.getKustoEngineUrl());
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
        return Version.getConnectorVersion();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        assignment.addAll(partitions);
        for (TopicPartition tp : assignment) {
            TopicIngestionProperties ingestionProps = getIngestionProps(tp.topic());
            LOGGER.debug("Open Kusto topic: '{}' with partition: '{}'", tp.topic(), tp.partition());
            if (ingestionProps == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped " +
                        "for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                IngestClient client = ingestionProps.streaming ? streamingIngestClient : kustoIngestClient;
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, client, ingestionProps, config, isDlqEnabled,
                        createKafkaRecordErrorReporter());
                writer.open();
                writers.put(tp, writer);
            }
        }
    }

    @Override
    public void close(@NotNull Collection<TopicPartition> partitions) {
        LOGGER.warn("Closing writers in KustoSinkTask");
        CountDownLatch countDownLatch = new CountDownLatch(partitions.size());
        // First stop so that no more ingestion trigger from timer flushes
        partitions.forEach((TopicPartition tp) -> writers.get(tp).stop());
        for (TopicPartition tp : partitions) {
            try {
                writers.get(tp).close();
                writers.remove(tp);
                assignment.remove(tp);
            } catch (ConnectException e) {
                LOGGER.error("Error closing topic partition for {}.", tp, e);
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    @Override
    public void start(Map<String, String> props) {
        config = new FabricSinkConfig(props);
        String url = config.getKustoIngestUrl();
        if (config.isDlqEnabled()) {
            isDlqEnabled = true;
        } else {
            isDlqEnabled = false;
        }
        topicsToIngestionProps = getTopicsToIngestionProps(config);
        // this should be read properly from settings
        createKustoIngestClient(config);
        LOGGER.info("Started KustoSinkTask with target cluster: ({}), source topics: ({})", url,
                topicsToIngestionProps.keySet());
        // Adding this check to make code testable
        if (context != null) {
            open(context.assignment());
        }
    }

    @Override
    public void stop() {
        LOGGER.warn("Stopping KustoSinkTask");
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
            LOGGER.error("Error closing kusto client", e);
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
                LOGGER.error("Error putting records: ", e);
                throw e;
            }
            if (sinkRecord.value() == null) {
                LOGGER.warn("Tombstone record at offset {}, key {} and partition {} ",
                        sinkRecord.kafkaOffset(), sinkRecord.key(), sinkRecord.kafkaPartition());
            }
            try {
                if (headerTransforms == null) {
                    headerTransforms = config.headerTransforms();
                }
            } catch (JsonProcessingException e) {
                LOGGER.warn("Error parsing HeaderTransforms field: ", e);
            }
            writer.writeRecord(sinkRecord, headerTransforms);
        }
        if (lastRecord != null) {
            LOGGER.debug("Last record offset: {}", lastRecord.kafkaOffset());
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
                LOGGER.debug("Forwarding to framework request to commit offset: {} for {} while the offset is {}", offset,
                        tp, offsets.get(tp));
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        }
        return offsetsToCommit;
    }

    /* Used to report a record back to DLQ if error tolerance is specified */
    protected KafkaRecordErrorReporter createKafkaRecordErrorReporter() {
        KafkaRecordErrorReporter errorReporter = isDlqEnabled ? new LegacyErrorReporter(config): new NoOpLoggerErrorReporter();
        if (context != null) {
            try {
                ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
                if (errantRecordReporter != null) {
                    errorReporter = (sinkRecord, error) -> {
                        try {
                            // Blocking this until record is delivered to DLQ
                            LOGGER.info(
                                    "Sending Sink Record to DLQ using Errant reporter recordOffset:{}, partition:{}",
                                    sinkRecord.kafkaOffset(),
                                    sinkRecord.kafkaPartition());
                            errantRecordReporter.report(sinkRecord, error).get();
                        } catch (InterruptedException | ExecutionException e) {
                            final String errMsg = "ERROR reporting records to ErrantRecordReporter";
                            throw new ConnectException(errMsg, e);
                        }
                    };
                } else {
                    LOGGER.info("Errant record reporter is not configured.");
                }
            } catch (NoClassDefFoundError | NoSuchMethodError e) {
                // Will occur in Connect runtimes earlier than 2.6
                LOGGER.info(
                        "Kafka versions prior to 2.6 do not support the errant record reporter.");
            }
        } else {
            LOGGER.warn("SinkTaskContext is not set , falling back to legacy error reporter");
        }
        return errorReporter;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // do nothing , rolling files can handle writing
    }
}
