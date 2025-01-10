package com.microsoft.fabric.connect.eventhouse.sink.formatwriter;

import java.io.OutputStream;

import com.microsoft.fabric.connect.eventhouse.sink.format.RecordWriter;
import com.microsoft.fabric.connect.eventhouse.sink.format.RecordWriterProvider;

public class EventHouseRecordWriterProvider implements RecordWriterProvider {
    @Override
    public RecordWriter getRecordWriter(String filename, OutputStream out) {
        return new EventHouseRecordWriter(filename, out);
    }
}
