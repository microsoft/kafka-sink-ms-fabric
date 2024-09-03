package com.microsoft.fabric.kafka.connect.sink.es;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class EventHubClientProvider {

    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4);

    private final ConnectionStringBuilder connectionStringBuilder;

    public EventHubClientProvider(String connectionString) {
        this.connectionStringBuilder = new ConnectionStringBuilder(connectionString);
    }

    public EventHubClient newInstance() throws EventHubException, IOException {
        return getEventHubClientFromConnectionString();
    }

    protected EventHubClient getEventHubClientFromConnectionString() throws EventHubException, IOException {
        return EventHubClient.createFromConnectionStringSync(this.connectionStringBuilder.toString(), executorService);
    }

}