package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
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

    public TopicPartitionWriter(TopicPartition tp, IngestClient client,
            @NotNull TopicIngestionProperties ingestionProps,
            @NotNull FabricSinkConfig config,
            boolean isDlqEnabled,
            @NotNull KafkaRecordErrorReporter errorReporter) {
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
    }

    static @NotNull String getTempDirectoryName(String tempDirPath) {
        String tempDir = String.format("kusto-sink-connector-%s", UUID.randomUUID());
        Path path = Paths.get(tempDirPath, tempDir).toAbsolutePath();
        return path.toString();
    }

    public void handleRollFile(@NotNull SourceFile fileDescriptor) {
        UUID fileSourceId = UUID.randomUUID();
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileSourceId);
        /*
         * Since retries can be for a longer duration the Kafka Consumer may leave the group. This will result in a new Consumer reading records from the last
         * committed offset leading to duplication of records in KustoDB. Also, if the error persists, it might also result in duplicate records being written
         * into DLQ topic. Recommendation is to set the following worker configuration as `connector.client.config.override.policy=All` and set the
         * `consumer.override.max.poll.interval.ms` config to a high enough value to avoid consumer leaving the group while the Connector is retrying.
         */
        this.ingestionRetry.executeTrySupplier(() -> Try.of(() -> this.client.ingestFromFile(fileSourceInfo, updateIngestionPropertiesWithTargetFormat())))
                .onSuccess(ingestionStatusResult -> {
                    this.lastCommittedOffset = currentOffset;
                    LOGGER.debug("Ingestion status: {} for file {} with ID {} .Committed offset {} ", ingestionStatusResult,
                            fileDescriptor.path, fileSourceId, lastCommittedOffset);
                })
                .onFailure(ex -> {
                    if (behaviorOnError != BehaviorOnError.FAIL) {
                        fileDescriptor.records.forEach(sinkRecord -> this.errorReporter.reportError(sinkRecord, new ConnectException(ex)));
                    }
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
                fabricSinkConfig);
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
