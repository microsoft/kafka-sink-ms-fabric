package com.microsoft.fabric.kafka.connect.sink.es;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import com.microsoft.fabric.kafka.connect.sink.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.codehaus.plexus.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static com.microsoft.fabric.kafka.connect.sink.es.EventStreamSinkConfig.ES_MESSAGE_FORMAT;

public class EventStreamSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(EventStreamSinkTask.class);
    // List of EventHubClient objects to be used during data upload
    private BlockingQueue<EventHubProducerAsyncClient> ehClients;
    private EventStreamSinkConfig eventStreamSinkConfig;

    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("starting EventHubSinkTask");
        try {
            this.eventStreamSinkConfig = new EventStreamSinkConfig(props);
        } catch (ConfigException ex) {
            throw new ConnectException("Couldn't start EventHubSinkTask due to configuration error", ex);
        }
        String connectionString = eventStreamSinkConfig.getString(EventStreamSinkConfig.ES_CONNECTION_STRING);
        log.info("connection string = {}", connectionString);
        short clientsPerTask = eventStreamSinkConfig.getShort(EventStreamSinkConfig.ES_CLIENTS_PER_TASK);
        log.info("clients per task = {}", clientsPerTask);
        initializeEventHubClients(connectionString, clientsPerTask);
    }

    @Override
    public void put(@NotNull Collection<SinkRecord> sinkRecords) {
        boolean isTextData = StringUtils.isNotEmpty(eventStreamSinkConfig.getString(ES_MESSAGE_FORMAT)) &&
                "json".equalsIgnoreCase(eventStreamSinkConfig.getString(ES_MESSAGE_FORMAT));
        log.debug("starting to upload {} records", sinkRecords.size());
        List<CompletableFuture<Void>> resultSet = new LinkedList<>();
        for (SinkRecord record : sinkRecords) {

            EventHubProducerAsyncClient ehClient = null;
            try {
                // pick an event hub client to send the data asynchronously
                // Creating a batch without options set, will allow for automatic routing of events to any partition.
                EventHubProducerAsyncClient finalEhClient = ehClients.take();
                // so that this can be offered back to the queue in finally
                ehClient = finalEhClient;
                finalEhClient.createBatch().flatMap(batch -> {
                    EventData sendEvent;
                    if (isTextData) {
                        sendEvent = new EventData(record.value().toString());
                    } else {
                        sendEvent = new EventData(record.va);
                    }
                    batch.tryAdd(sendEvent);
                    return finalEhClient.send(batch);
                }).subscribe(unused -> {
                },
                        error -> log.error("Error sending batch of records", error),
                        () -> log.debug("Records sent successfully"));

            } catch (InterruptedException ex) {
                throw new ConnectException("EventHubSinkTask interrupted while waiting to acquire client", ex);
            } finally {
                if (ehClient != null) {
                    boolean offerResult = ehClients.offer(ehClient);
                }
            }
        }
        log.debug("finished uploading {} records", sinkRecords.size());
    }

    @Override
    public void flush(@NotNull Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((topicPartition, offsetAndMetadata) -> log.debug("flushing topic partition {} with offset {}",
                topicPartition, offsetAndMetadata));
    }

    @Override
    public void stop() {
        log.info("stopping EventHubSinkTask");
        if (ehClients != null) {
            for (EventHubProducerAsyncClient ehClient : ehClients) {
                ehClient.close();
                log.info("closing an Event hub Client");
            }
        }
    }

    private void initializeEventHubClients(String connectionString, short clientsPerTask) {
        ehClients = new LinkedBlockingQueue<>(clientsPerTask);
        EventHubClientProvider provider = getClientProvider(connectionString);
        try {
            for (short i = 0; i < clientsPerTask; i++) {
                log.info("Creating event hub client - {} connectionString={} - ", i,
                        EventStreamCommon.getSecureConnectionString(connectionString));
                boolean offerResult = ehClients.offer(provider.newInstance());
                if (!offerResult) {
                    log.error("Error occurred while creating EventHub client");
                    throw new ConnectException("Error occurred while creating EventHub client");
                }
                log.info("Created an Event Hub Client");
            }
        } catch (Exception ex) {
            throw new ConnectException("Error while connecting to EventHubs", ex);
        }
    }

    protected EventHubClientProvider getClientProvider(String connectionString) {
        return new EventHubClientProvider(connectionString);
    }

}
