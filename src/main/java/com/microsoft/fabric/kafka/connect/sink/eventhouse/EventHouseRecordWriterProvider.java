package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.microsoft.fabric.kafka.connect.sink.format.RecordWriter;
import com.microsoft.fabric.kafka.connect.sink.format.RecordWriterProvider;

import java.io.OutputStream;

public class EventHouseRecordWriterProvider implements RecordWriterProvider {
    @Override
    public RecordWriter getRecordWriter(String filename, OutputStream out) {
        return new EventHouseRecordWriter(out);
    }
}
