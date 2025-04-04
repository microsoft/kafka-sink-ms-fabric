package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.IngestionStatusResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConfig.BehaviorOnError;
import com.microsoft.fabric.connect.eventhouse.sink.dlq.KafkaRecordErrorReporter;
import com.microsoft.fabric.connect.eventhouse.sink.formatwriter.FormatWriterHelper;
import com.microsoft.fabric.connect.eventhouse.sink.metrics.FabricKafkaMetricsUtil;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vavr.control.Try;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat.MULTIJSON;

public class TopicPartitionWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionWriter.class);
    private static final String COMPRESSION_EXTENSION = ".gz";
    private static final String FILE_EXCEPTION_MESSAGE = "Failed to create file or write record into file for ingestion.";
    private final TopicPartition tp;
    private final IngestClient client;
    private final TopicIngestionProperties ingestionProps;
    private final String basePath;
    private final long flushInterval;
    private final long fileThreshold;
    private final BehaviorOnError behaviorOnError;
    private final ReentrantReadWriteLock reentrantReadWriteLock;
    FileWriter fileWriter;
    long currentOffset;
    Long lastCommittedOffset;
    private final KafkaRecordErrorReporter errorReporter;
    private final boolean isDlqEnabled;
    private final Retry ingestionRetry;
    private final FabricSinkConfig fabricSinkConfig;
    private final MetricRegistry metricRegistry;
    private Counter fileCountOnIngestion;
    private Counter fileCountTableStageIngestionFail;
    private Counter ingestionErrorCount;
    private Counter ingestionSuccessCount;
    private Timer commitLag;
    private Timer ingestionLag;
    private long writeTime;

    public TopicPartitionWriter(TopicPartition tp, IngestClient client,
            @NotNull TopicIngestionProperties ingestionProps,
            @NotNull FabricSinkConfig config,
            boolean isDlqEnabled,
            @NotNull KafkaRecordErrorReporter errorReporter,
            @NotNull MetricRegistry metricRegistry) {
        this.tp = tp;
        this.client = client;
        this.ingestionProps = ingestionProps;
        this.fileThreshold = config.getFlushSizeBytes();
        this.basePath = getTempDirectoryName(config.getTempDirPath());
        this.flushInterval = config.getFlushInterval();
        this.currentOffset = 0;
        this.reentrantReadWriteLock = new ReentrantReadWriteLock(true);
        long retryBackOffTime = config.getRetryBackOffTimeMs();
        this.behaviorOnError = config.getBehaviorOnError();
        this.errorReporter = errorReporter;
        this.isDlqEnabled = isDlqEnabled;
        this.fabricSinkConfig = config;
        this.metricRegistry = metricRegistry;

        IntervalFunction sleepConfig = IntervalFunction.ofExponentialRandomBackoff(
                retryBackOffTime,
                IntervalFunction.DEFAULT_MULTIPLIER,
                IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
                retryBackOffTime * 2);

        int retryAttempts = ingestionProps.streaming ? 1 : config.getMaxRetryAttempts();
        RetryConfig retryConfig = RetryConfig.custom().intervalFunction(sleepConfig).retryOnResult(
                /* Retry the streaming ingest failures */
                ingestionStatusResult -> ingestionStatusResult instanceof IngestionStatusResult
                        && !((IngestionStatusResult) ingestionStatusResult).getIngestionStatusCollection().isEmpty()
                        && hasStreamingIngestionFailed(((IngestionStatusResult) ingestionStatusResult).getIngestionStatusCollection().get(0)))
                .retryOnException(ex -> ex instanceof IngestionServiceException && isPermanentException((IngestionServiceException) ex))
                .maxAttempts(retryAttempts).build();
        this.ingestionRetry = Retry.of("ingestionRetry", retryConfig);

        initializeMetrics(tp.topic(), metricRegistry);
    }
    private void initializeMetrics(String topic, MetricRegistry metricRegistry) {
        this.fileCountOnIngestion = metricRegistry.counter(FabricKafkaMetricsUtil.constructMetricName(topic, FabricKafkaMetricsUtil.FILE_COUNT_SUB_DOMAIN, FabricKafkaMetricsUtil.FILE_COUNT_ON_INGESTION));
        this.fileCountTableStageIngestionFail = metricRegistry.counter(FabricKafkaMetricsUtil.constructMetricName(topic, FabricKafkaMetricsUtil.FILE_COUNT_SUB_DOMAIN, FabricKafkaMetricsUtil.FILE_COUNT_TABLE_STAGE_INGESTION_FAIL));
        this.ingestionErrorCount = metricRegistry.counter(FabricKafkaMetricsUtil.constructMetricName(topic, FabricKafkaMetricsUtil.DLQ_SUB_DOMAIN, FabricKafkaMetricsUtil.INGESTION_ERROR_COUNT));
        this.ingestionSuccessCount = metricRegistry.counter(FabricKafkaMetricsUtil.constructMetricName(topic, FabricKafkaMetricsUtil.DLQ_SUB_DOMAIN, FabricKafkaMetricsUtil.INGESTION_SUCCESS_COUNT));
        this.commitLag = metricRegistry.timer(FabricKafkaMetricsUtil.constructMetricName(topic, FabricKafkaMetricsUtil.LATENCY_SUB_DOMAIN, FabricKafkaMetricsUtil.EventType.COMMIT_LAG.getMetricName()));
        this.ingestionLag = metricRegistry.timer(FabricKafkaMetricsUtil.constructMetricName(topic, FabricKafkaMetricsUtil.LATENCY_SUB_DOMAIN, FabricKafkaMetricsUtil.EventType.INGESTION_LAG.getMetricName()));
        
        String processedOffsetMetricName = FabricKafkaMetricsUtil.constructMetricName(topic, FabricKafkaMetricsUtil.OFFSET_SUB_DOMAIN, FabricKafkaMetricsUtil.PROCESSED_OFFSET);
        if (!metricRegistry.getGauges().containsKey(processedOffsetMetricName)) {
            metricRegistry.register(processedOffsetMetricName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return currentOffset;
                }
            });
        }
    
        String committedOffsetMetricName = FabricKafkaMetricsUtil.constructMetricName(topic, FabricKafkaMetricsUtil.OFFSET_SUB_DOMAIN, FabricKafkaMetricsUtil.COMMITTED_OFFSET);
        if (!metricRegistry.getGauges().containsKey(committedOffsetMetricName)) {
            metricRegistry.register(committedOffsetMetricName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return lastCommittedOffset != null ? lastCommittedOffset : 0L;
                }
            });
        }
    }

    static @NotNull String getTempDirectoryName(String tempDirPath) {
        String tempDir = String.format("kusto-sink-connector-%s", UUID.randomUUID());
        Path path = Paths.get(tempDirPath, tempDir).toAbsolutePath();
        return path.toString();
    }

    public void handleRollFile(@NotNull SourceFile fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);
        if (writeTime == 0) {
            LOGGER.warn("writeTime is not initialized properly before invoking handleRollFile. Setting it to the current time.");
            writeTime = System.currentTimeMillis(); // Initialize writeTime if not already set
        }
        fileCountOnIngestion.inc();
        long uploadStartTime = System.currentTimeMillis(); // Record the start time of file upload
        commitLag.update(uploadStartTime - writeTime, TimeUnit.MILLISECONDS);
        /*
         * Since retries can be for a longer duration the Kafka Consumer may leave the group. This will result in a new Consumer reading records from the last
         * committed offset leading to duplication of records in KustoDB. Also, if the error persists, it might also result in duplicate records being written
         * into DLQ topic. Recommendation is to set the following worker configuration as `connector.client.config.override.policy=All` and set the
         * `consumer.override.max.poll.interval.ms` config to a high enough value to avoid consumer leaving the group while the Connector is retrying.
         */
        this.ingestionRetry.executeTrySupplier(() -> Try.of(() -> this.client.ingestFromFile(fileSourceInfo, updateIngestionPropertiesWithTargetFormat())))
                .onSuccess(ingestionStatusResult -> {
                    this.lastCommittedOffset = currentOffset;
                    LOGGER.debug("Ingestion status: {} for file {}.Committed offset {} ", ingestionStatusResult,
                            fileDescriptor.path, lastCommittedOffset);
                    ingestionSuccessCount.inc(); // Increment the ingestion success counter
                    fileCountOnIngestion.dec(); // Decrement the file count on ingestion counter
                    long ingestionEndTime = System.currentTimeMillis(); // Record the end time of ingestion
                    ingestionLag.update(ingestionEndTime - uploadStartTime, TimeUnit.MILLISECONDS); // Update ingestion-lag            
                })
                .onFailure(ex -> {
                    if (behaviorOnError != BehaviorOnError.FAIL) {
                        fileDescriptor.records.forEach(sinkRecord -> this.errorReporter.reportError(sinkRecord, new ConnectException(ex)));
                    }
                    ingestionErrorCount.inc(); // Increment the ingestion error counter
                    fileCountOnIngestion.dec(); // Decrement the file count on ingestion counter
                    fileCountTableStageIngestionFail.inc(); // Increment the file count table stage ingestion fail counter
                });
    }

    private static boolean isPermanentException(@NotNull IngestionServiceException exception) {
        Throwable innerException = exception.getCause();
        return innerException instanceof KustoDataExceptionBase &&
                (((KustoDataExceptionBase) innerException).isPermanent());
    }

    private boolean hasStreamingIngestionFailed(@NotNull IngestionStatus status) {
        switch (status.status) {
            case Succeeded:
            case Queued:
            case Pending:
                return false;
            case Skipped:
            case PartiallySucceeded:
                String failureStatus = status.getFailureStatus();
                String details = status.getDetails();
                UUID ingestionSourceId = status.getIngestionSourceId();
                LOGGER.warn("A batch of streaming records has {} ingestion: table:{}, database:{}, operationId: {}," +
                        "ingestionSourceId: {}{}{}.\n" +
                        "Status is final and therefore ingestion won't be retried and data won't reach dlq",
                        status.getStatus(),
                        status.getTable(),
                        status.getDatabase(),
                        status.getOperationId(),
                        ingestionSourceId,
                        (StringUtils.isNotEmpty(failureStatus) ? (", failure: " + failureStatus) : ""),
                        (StringUtils.isNotEmpty(details) ? (", details: " + details) : ""));
                return false;
            case Failed:
                LOGGER.error("A batch of streaming records has failed ingestion: table:{}, database:{}, operationId: {}," +
                        "ingestionSourceId: {}, failure: {}, details: {}",
                        status.getTable(),
                        status.getDatabase(),
                        status.getOperationId(),
                        status.getIngestionSourceId(),
                        status.getFailureStatus(),
                        status.getDetails());
                return true;
        }
        return true;
    }

    String getFilePath(@Nullable Long offset) {
        // Should be null if flushed by interval
        offset = offset == null ? currentOffset : offset;
        long nextOffset = fileWriter != null && fileWriter.isDirty() ? offset + 1 : offset;

        return Paths.get(basePath, String.format("kafka_%s_%s_%d.%s%s", tp.topic(), tp.partition(), nextOffset,
                ingestionProps.ingestionProperties.getDataFormat(), COMPRESSION_EXTENSION)).toString();
    }

    void writeRecord(SinkRecord sinkRecord, HeaderTransforms headerTransforms) throws ConnectException {
        if (sinkRecord != null) {
            try (AutoCloseableLock ignored = new AutoCloseableLock(reentrantReadWriteLock.readLock())) {
                this.currentOffset = sinkRecord.kafkaOffset();
                fileWriter.writeData(sinkRecord, headerTransforms);
                writeTime = System.currentTimeMillis();
            } catch (IOException | DataException ex) {
                handleErrors(sinkRecord, ex);
            }
        }
    }

    private void handleErrors(SinkRecord sinkRecord, Exception ex) {
        if (BehaviorOnError.FAIL == behaviorOnError) {
            throw new ConnectException(FILE_EXCEPTION_MESSAGE, ex);
        } else {
            LOGGER.debug(FILE_EXCEPTION_MESSAGE, ex);
            this.errorReporter.reportError(sinkRecord, ex);
        }
    }

    void open() {
        // Should compress binary files
        fileWriter = new FileWriter(
                basePath,
                fileThreshold,
                this::handleRollFile,
                this::getFilePath,
                flushInterval,
                reentrantReadWriteLock,
                ingestionProps.ingestionProperties.getDataFormat(),
                behaviorOnError,
                isDlqEnabled,
                fabricSinkConfig,
                tp.topic(),
                metricRegistry);
    }

    void close() {
        try {
            fileWriter.rollback();
            fileWriter.close();
        } catch (IOException e) {
            LOGGER.error("Failed to rollback with exception={0}", e);
        }
        try {
            FileUtils.deleteDirectory(new File(basePath));
        } catch (IOException e) {
            LOGGER.error("Unable to delete temporary connector folder {}", basePath);
        }
    }

    void stop() {
        fileWriter.stop();
    }

    private @NotNull IngestionProperties updateIngestionPropertiesWithTargetFormat() {
        IngestionProperties updatedIngestionProperties = new IngestionProperties(this.ingestionProps.ingestionProperties);
        IngestionProperties.DataFormat sourceFormat = ingestionProps.ingestionProperties.getDataFormat();
        if (FormatWriterHelper.INSTANCE.isSchemaFormat(sourceFormat)) {
            LOGGER.debug("Incoming dataformat {}, setting target format to MULTIJSON", sourceFormat);
            updatedIngestionProperties.setDataFormat(MULTIJSON);
        } else {
            updatedIngestionProperties.setDataFormat(ingestionProps.ingestionProperties.getDataFormat());
        }
        // Just to make it clear , split the conditional
        if (FormatWriterHelper.INSTANCE.isSchemaFormat(sourceFormat)) {
            IngestionMapping mappingReference = ingestionProps.ingestionProperties.getIngestionMapping();
            if (mappingReference != null && StringUtils.isNotEmpty(mappingReference.getIngestionMappingReference())) {
                String ingestionMappingReferenceName = mappingReference.getIngestionMappingReference();
                updatedIngestionProperties.setIngestionMapping(ingestionMappingReferenceName, IngestionMapping.IngestionMappingKind.JSON);
            }
        }
        return updatedIngestionProperties;
    }
}
