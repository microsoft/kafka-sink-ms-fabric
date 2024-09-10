package com.microsoft.fabric.kafka.connect.sink.es;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;

public class EventHubClientProvider {
    private final String connectionStringBuilder;

    public EventHubClientProvider(String connectionString) {
        this.connectionStringBuilder = connectionString;
    }

    public EventHubProducerAsyncClient newInstance() {
        return new EventHubClientBuilder()
                .connectionString(connectionStringBuilder)
                .buildAsyncProducerClient();
    }
}
