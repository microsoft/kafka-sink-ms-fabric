package com.microsoft.fabric.kafka.connect.sink.es;

import com.microsoft.azure.eventhubs.AuthorizationFailedException;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.fabric.kafka.connect.sink.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public class EventStreamSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(EventStreamSinkTask.class);
    // List of EventHubClient objects to be used during data upload
    private BlockingQueue<EventHubClient> ehClients;
    private EventHubClientProvider provider;

    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("starting EventHubSinkTask");
        EventStreamSinkConfig eventStreamSinkConfig;
        try {
            eventStreamSinkConfig = new EventStreamSinkConfig(props);
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
        log.debug("starting to upload {} records", sinkRecords.size());
        List<CompletableFuture<Void>> resultSet = new LinkedList<>();
        for (SinkRecord record : sinkRecords) {
            EventData sendEvent;
            EventHubClient ehClient = null;
            try {
                sendEvent = extractor.extractEventData(record);
                // pick an event hub client to send the data asynchronously
                ehClient = ehClients.take();
                if (sendEvent != null) {
                    resultSet.add(sendAsync(ehClient, sendEvent));
                }
            } catch (InterruptedException ex) {
                throw new ConnectException("EventHubSinkTask interrupted while waiting to acquire client", ex);
            } finally {
                if (ehClient != null) {
                    boolean offerResult = ehClients.offer(ehClient);
                }
            }
        }
        log.debug("wait for {} async uploads to finish", resultSet.size());
        waitForAllUploads(resultSet);
        log.debug("finished uploading {} records", sinkRecords.size());
    }

    @Override
    public void flush(@NotNull Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((topicPartition, offsetAndMetadata) ->
                log.debug("flushing topic partition {} with offset {}", topicPartition, offsetAndMetadata));
    }

    @Override
    public void stop() {
        log.info("stopping EventHubSinkTask");
        if (ehClients != null) {
            for (EventHubClient ehClient : ehClients) {
                ehClient.close();
                log.info("closing an Event hub Client");
            }
        }
    }

    protected CompletableFuture<Void> sendAsync(@NotNull EventHubClient ehClient, EventData sendEvent) {
        return ehClient.send(sendEvent);
    }

    private void initializeEventHubClients(String connectionString, short clientsPerTask) {
        ehClients = new LinkedBlockingQueue<>(clientsPerTask);
        provider = getClientProvider(connectionString);
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
        } catch (AuthorizationFailedException ex) {
            log.error("Authorization failed while connecting to EventHub [connectionString={}]", EventStreamCommon.getSecureConnectionString(connectionString));
            throw new ConnectException("Authorization error. Unable to connect to EventHub", ex);
        } catch (EventHubException ex) {
            log.error("Error occurred while connecting to EventHub [connectionString={}]", EventStreamCommon.getSecureConnectionString(connectionString));
            throw new ConnectException("Exception while creating Event Hub client: " + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new ConnectException("Error while connecting to EventHubs", ex);
        }
    }

    protected EventHubClientProvider getClientProvider(String connectionString) {
        return new EventHubClientProvider(connectionString);
    }

    private void waitForAllUploads(@NotNull List<CompletableFuture<Void>> resultSet) {
        for (CompletableFuture<Void> result : resultSet) {
            try {
                try {
                    result.get();
                } catch (ExecutionException | InterruptedException ex) {
                    findValidRootCause(ex);
                }
            } catch (AuthorizationFailedException ex) {
                log.error("Authorization failed while sending events to EventHub");
                throw new ConnectException("Authorization error. Unable to connect to EventHub", ex);
            } catch (EventHubException ex) {
                log.error("Error occurred while sending events to EventHub");
                throw new ConnectException("Exception while creating Event Hub client: " + ex.getMessage(), ex);
            } catch (IOException ex) {
                throw new ConnectException("Error while connecting to EventHubs", ex);
            }
        }
    }

    private void findValidRootCause(Exception ex) throws EventHubException, IOException {
        Throwable rootCause = ex;
        while (rootCause != null) {
            rootCause = rootCause.getCause();
            if (rootCause instanceof IOException) {
                throw (IOException) rootCause;
            } else if (rootCause instanceof EventHubException) {
                throw (EventHubException) rootCause;
            }
        }
        throw new IOException("Error while executing send", ex);
    }
}
