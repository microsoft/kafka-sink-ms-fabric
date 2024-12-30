package com.microsoft.fabric.connect.eventhouse.sink.format;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.connect.sink.SinkRecord;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.connect.eventhouse.sink.HeaderTransforms;

public interface RecordWriter extends Closeable {
    /**
     * Write a sinkRecord to storage.
     *
     * @param sinkRecord the sinkRecord to persist.
     */
    void write(SinkRecord sinkRecord, IngestionProperties.DataFormat dataFormat, HeaderTransforms headerTransforms) throws IOException;

    /**
     * Close this writer.
     */
    void close();

    /**
     * Flush writer's data and commit the records in Kafka. Optionally, this operation might also
     * close the writer.
     */
    void commit();
}
