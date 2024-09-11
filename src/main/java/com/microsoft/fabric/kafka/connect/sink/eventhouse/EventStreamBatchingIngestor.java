package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.microsoft.fabric.kafka.connect.sink.es.EventStreamSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class EventStreamBatchingIngestor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamBatchingIngestor.class);
    private final EventHouseSinkConfig config;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient ScheduledExecutorService scheduler;
    private transient volatile Exception flushException;
    private final ConcurrentHashMap<String,List<String>> topicToMessageBuffer = new ConcurrentHashMap<>();
    private transient volatile boolean closed = false;
    private transient volatile boolean checkpointInProgress = false;
    private final Map<String,TopicToTableMapping> topicToIngestion;

    public EventStreamBatchingIngestor(@NotNull EventHouseSinkConfig config) {
        this.config = config;
        this.topicToIngestion = Arrays.stream(config.getTopicToTableMapping()).collect(Collectors.toMap(TopicToTableMapping::getTopic, t -> t));
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
    }

    void doBulkWrite() throws IOException {
        if (topicToMessageBuffer.isEmpty()) {
            // no records to write
            LOGGER.debug("No records to write to DB {} & table {} ", writeOptions.getDatabase(),
                    writeOptions.getTable());
            return;
        }
        LOGGER.info("Ingesting to DB {} & table {} record count {}", writeOptions.getDatabase(),
                writeOptions.getTable(), bulkRequests.size());
        if (this.kustoSinkCommon.ingest(this.bulkRequests)) {
            // All the ingestion has completed successfully here. Clear this batch of records
            this.topicToMessageBuffer.clear();
        }
    }

    private boolean isOverMaxBatchIntervalLimit() {
        long bulkFlushInterval = this.config.getFlushInterval();
        long lastSentInterval = Instant.now(Clock.systemUTC()).toEpochMilli() - this.kustoSinkCommon.lastSendTime;
        boolean isOverIntervalLimit = this.kustoSinkCommon.lastSendTime >= 0 && bulkFlushInterval != -1
                && lastSentInterval >= bulkFlushInterval;
        if (isOverIntervalLimit) {
            LOGGER.trace(
                    "OverMaxBatchIntervalLimit triggered at {}. LastSentTime {}.The last sent interval is {} and bulkFlushInterval {}.",
                    Instant.now(Clock.systemUTC()), Instant.ofEpochMilli(this.kustoSinkCommon.lastSendTime),
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
        checkpointInProgress = true;
        while (!topicToMessageBuffer.isEmpty()) {
            doBulkWrite();
        }
        checkpointInProgress = false;
    }

    public synchronized void close() throws Exception {
        if (!closed) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduler.shutdown();
            }
            if (!topicToMessageBuffer.isEmpty()) {
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
}
