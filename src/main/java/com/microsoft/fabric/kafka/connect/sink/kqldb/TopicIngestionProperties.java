package com.microsoft.fabric.kafka.connect.sink.kqldb;

import com.microsoft.azure.kusto.ingest.IngestionProperties;

public class TopicIngestionProperties {
    public IngestionProperties ingestionProperties;
    boolean streaming;
}
