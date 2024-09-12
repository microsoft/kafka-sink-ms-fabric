package com.microsoft.fabric.kafka.connect.sink.format;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;
import java.io.IOException;

public interface RecordWriter extends Closeable {
    /**
     * Write a record to storage.
     *
     * @param record the record to persist.
     */
    String write(SinkRecord record, IngestionProperties.DataFormat dataFormat) throws IOException;

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
