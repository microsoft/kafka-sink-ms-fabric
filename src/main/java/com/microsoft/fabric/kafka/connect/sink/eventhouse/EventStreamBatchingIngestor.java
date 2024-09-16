package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.fabric.kafka.connect.sink.eventhouse.internal.EventHouseClientUtil;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EventStreamBatchingIngestor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamBatchingIngestor.class);
    private final EventHouseSinkConfig config;
    private final List<String> bulkRequests = new ArrayList<>();
    private final TopicToTableMapping topicToTableMap;
    private final IngestClient ingestClient;
    private final ScheduledExecutorService pollResultsExecutor =
            Executors.newSingleThreadScheduledExecutor();
    private final Retry retry;
    private volatile long lastSendTime = Instant.now(Clock.systemUTC()).toEpochMilli();
    private transient ScheduledFuture<?> scheduledFuture;
    private transient ScheduledExecutorService scheduler;
    private transient volatile Exception flushException;
    private transient volatile boolean closed = false;
    private transient volatile boolean checkpointInProgress = false;
    private final TopicPartition topicPartition;

    public EventStreamBatchingIngestor(@NotNull EventHouseSinkConfig config,
                                       @NotNull TopicToTableMapping topicToTableMap,
                                        @NotNull TopicPartition topicPartition) {
        try {
            this.config = config;
            this.ingestClient = EventHouseClientUtil.createIngestClient(config, topicToTableMap.isStreaming());
            this.topicToTableMap = topicToTableMap;
            this.topicPartition = topicPartition;
            this.retry = getRetries(config);
            if (config.getFlushInterval() > 0) {
                this.scheduler = Executors.newScheduledThreadPool(1);
                this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                    synchronized (EventStreamBatchingIngestor.this) {
                        if (!closed && isOverMaxBatchIntervalLimit()) {
                            try {
                                doBulkWrite();
                            } catch (Exception e) {
                                flushException = e;
                            }
                        }
                    }
                }, config.getFlushInterval(), config.getFlushInterval(), TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LOGGER.error("Error while initializing EventStreamBatchingIngestor", e);
            throw new ConnectException("Error while initializing EventStreamBatchingIngestor", e);
        }
    }

    void write(List<String> records) throws IOException {
        checkFlushException();
        while (this.checkpointInProgress) {
            Thread.yield();
        }
        this.bulkRequests.addAll(records);
        if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
            doBulkWrite();
        }
    }

    private boolean isOverMaxBatchSizeLimit() {
        long bulkActions = this.config.getMaxRecords();
        boolean isOverMaxBatchSizeLimit = bulkActions != -1 && this.bulkRequests.size() >= bulkActions;
        if (isOverMaxBatchSizeLimit) {
            LOGGER.debug("OverMaxBatchSizeLimit triggered at time {} with batch size {}.",
                    Instant.now(Clock.systemUTC()), this.bulkRequests.size());
        }
        return isOverMaxBatchSizeLimit;
    }


    private void doBulkWrite() throws IOException {
        if (this.bulkRequests.isEmpty()) {
            // no records to write
            LOGGER.debug("No records to write to DB {} & table {} ", this.topicToTableMap.getDb(),
                    this.topicToTableMap.getTable());
            return;
        }
        LOGGER.info("Ingesting to DB {} & table {} record count {}", this.topicToTableMap.getDb(),
                this.topicToTableMap.getTable(), this.bulkRequests.size());
        if (ingest()) {
            // All the ingestion has completed successfully here. Clear this batch of records
            this.bulkRequests.clear();
        }
    }

    private boolean isOverMaxBatchIntervalLimit() {
        long bulkFlushInterval = this.config.getFlushInterval();
        long lastSentInterval = Instant.now(Clock.systemUTC()).toEpochMilli() - this.lastSendTime;
        boolean isOverIntervalLimit = this.lastSendTime >= 0 && bulkFlushInterval != -1
                && lastSentInterval >= bulkFlushInterval;
        if (isOverIntervalLimit) {
            LOGGER.trace(
                    "OverMaxBatchIntervalLimit triggered at {}. LastSentTime {}.The last sent interval is {} and bulkFlushInterval {}.",
                    Instant.now(Clock.systemUTC()), Instant.ofEpochMilli(this.lastSendTime),
                    lastSentInterval, bulkFlushInterval);
        }
        return isOverIntervalLimit;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Kusto failed.", flushException);
        }
    }

    public void flush() throws IOException {
        checkFlushException();
        this.checkpointInProgress = true;
        while (!this.bulkRequests.isEmpty()) {
            doBulkWrite();
        }
        this.checkpointInProgress = false;
    }

    public synchronized void close() throws IOException {
        if (!closed) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduler.shutdown();
            }
            if (!this.bulkRequests.isEmpty()) {
                try {
                    doBulkWrite();
                } catch (Exception e) {
                    LOGGER.error("Writing records to Kusto failed when closing KustoWriter", e);
                    throw new IOException("Writing records to Kusto failed.", e);
                } finally {
                    closed = true;
                }
            } else {
                closed = true;
            }
        }
    }

    @Contract(pure = true)
    @NotNull
    protected Supplier<IngestionResult> performIngestSupplier(InputStream inputDataStream, UUID sourceId) {
        return () -> {
            try {
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputDataStream, false, sourceId, CompressionType.gz);
                LOGGER.trace("Ingesting into table: {} with source id {}", this.topicToTableMap.getTable(), sourceId);
                IngestionProperties ingestionProperties =
                        new IngestionProperties(this.topicToTableMap.getDb(), this.topicToTableMap.getTable());
                ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
                ingestionProperties
                        .setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
                ingestionProperties.setDataFormat(topicToTableMap.getFormat());
                if (StringUtils.isNotEmpty(this.topicToTableMap.getMapping())) {
                    ingestionProperties.setIngestionMapping(this.topicToTableMap.getMapping(),
                            IngestionMapping.IngestionMappingKind.CSV);
                }
                // This is when the last upload and queue request happened
                this.lastSendTime = Instant.now(Clock.systemUTC()).toEpochMilli();
                LOGGER.trace("Setting last send time to {}", this.lastSendTime);
                return this.ingestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
            } catch (IngestionClientException | IngestionServiceException e) {
                String errorMessage = String
                        .format("URI syntax exception polling ingestion status for sourceId: %s", sourceId);
                LOGGER.warn(errorMessage, e);
                throw new RuntimeException(errorMessage, e);
            }
        };
    }

    boolean ingest() throws IOException {

        // Get the blob
        // Write to the blob
        // have the ingest client send out request for ingestion
        // wait for result of the ingest and return true/false
        if (this.bulkRequests.isEmpty()) {
            return true;
        }
        UUID sourceId = UUID.randomUUID();
        // Is a side effect. Can be a bit more polished, it is easier to send the total metric in one go.
        try (PipedInputStream pipedInputStream = new PipedInputStream();
             PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(pipedOutputStream))
        ) {
            for (String request : this.bulkRequests) {
                writer.write(request);
                writer.newLine();
            }
            return uploadAndPollStatus(pipedInputStream, sourceId);
        } catch (IOException e) {
            LOGGER.error("Error (IOException) while writing to blob.", e);
            throw e;
        }
    }

    private boolean uploadAndPollStatus(InputStream dataStream, UUID sourceId) {
        IngestionResult ingestionResult = retry.executeSupplier(this.performIngestSupplier(dataStream, sourceId));
        try {
            final Long ingestionStart = Instant.now(Clock.systemUTC()).toEpochMilli();
            final String pollResult =
                    this.pollForCompletion(sourceId.toString(), ingestionResult, ingestionStart)
                            .get();
            return OperationStatus.Succeeded.name().equals(pollResult);
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error while polling for completion of ingestion.", e);
            throw new ConnectException(e);
        }
    }

    protected CompletableFuture<String> pollForCompletion(String sourceId,
                                                          IngestionResult ingestionResult, Long ingestionStart) {
        CompletableFuture<String> completionFuture = new CompletableFuture<>();
        // TODO: Should this be configurable?
        long timeToEndPoll = Instant.now(Clock.systemUTC()).plus(5, ChronoUnit.MINUTES).toEpochMilli();
        final ScheduledFuture<?> checkFuture = pollResultsExecutor.scheduleAtFixedRate(() -> {
            if (Instant.now(Clock.systemUTC()).toEpochMilli() > timeToEndPoll) {
                LOGGER.warn(
                        "Time for polling end is {} and the current epoch time is {}. This operation will timeout",
                        Instant.ofEpochMilli(timeToEndPoll), Instant.now(Clock.systemUTC()));
                completionFuture.completeExceptionally(new TimeoutException(
                        "Polling for ingestion of source id: " + sourceId + " timed out."));
            }
            try {
                LOGGER.debug("Ingestion Status {} for id {}",
                        ingestionResult.getIngestionStatusCollection().stream()
                                .map(is -> is.getIngestionSourceId() + ":" + is.getStatus())
                                .collect(Collectors.joining(",")),
                        sourceId);
                ingestionResult.getIngestionStatusCollection().stream().filter(
                                ingestionStatus -> ingestionStatus.getIngestionSourceId().toString().equals(sourceId))
                        .findFirst().ifPresent(ingestionStatus -> {
                            if (ingestionStatus.status == OperationStatus.Succeeded) {
                                completionFuture.complete(ingestionStatus.status.name());
                                LOGGER.info("Ingestion for blob {} took {} ms for state change to Succeeded", sourceId,
                                        Instant.now(Clock.systemUTC()).toEpochMilli() - ingestionStart);
                            } else if (ingestionStatus.status == OperationStatus.Failed) {
                                String failureReason = String.format(
                                        "Ingestion failed for sourceId: %s with failure reason %s.",
                                        sourceId, ingestionStatus.getFailureStatus());
                                LOGGER.error(failureReason);
                                completionFuture.completeExceptionally(new RuntimeException(failureReason));
                            } else if (ingestionStatus.status == OperationStatus.PartiallySucceeded) {
                                // TODO check if this is really the case. What happens if one the blobs was
                                // malformed ?
                                String failureReason = String.format(
                                        "Ingestion partially succeeded for sourceId: %s with failure reason %s. "
                                                + "This will result in duplicates if the error was transient and was retried."
                                                + "Operation took %d ms",
                                        sourceId, ingestionStatus.getFailureStatus(),
                                        (Instant.now(Clock.systemUTC()).toEpochMilli() - ingestionStart));
                                LOGGER.warn(failureReason);
                                completionFuture.complete(ingestionStatus.status.name());
                            }
                        });
            } catch (URISyntaxException e) {
                String errorMessage = String
                        .format("URI syntax exception polling ingestion status for sourceId: %s", sourceId);
                LOGGER.warn(errorMessage, e);
                completionFuture.completeExceptionally(new RuntimeException(errorMessage, e));
            }
        }, 1, 5, TimeUnit.SECONDS); // TODO pick up from write options. Also CRP needs to be picked
        // up
        completionFuture.whenComplete((result, thrown) -> checkFuture.cancel(true));
        return completionFuture;
    }

    @NotNull
    private Retry getRetries(@NotNull EventHouseSinkConfig config) {
        final Retry retry;
        IntervalFunction backOffFunction = IntervalFunction.ofExponentialRandomBackoff(
                config.getRetryBackOffTimeMillis(), IntervalFunction.DEFAULT_MULTIPLIER,
                IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR, config.getMaxIntervalMillis());
        Predicate<Throwable> isTransientException = e -> {
            if ((e instanceof KustoDataExceptionBase)) {
                return !((KustoDataExceptionBase) e).isPermanent();
            }
            return e instanceof IngestionServiceException;
        };
        RetryConfig retryConfig = RetryConfig.custom().maxAttempts(config.getRetryAttempts())
                .intervalFunction(backOffFunction).retryOnException(isTransientException).build();
        RetryRegistry registry = RetryRegistry.of(retryConfig);
        retry = registry.retry("tempStoreService", retryConfig);
        return retry;
    }
}
