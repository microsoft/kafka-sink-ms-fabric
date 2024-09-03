package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.microsoft.azure.kusto.ingest.IngestionProperties;

public class TopicIngestionProperties {
    public IngestionProperties ingestionProperties;
    boolean streaming;
}
