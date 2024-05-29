package com.microsoft.fabric.kafka.connect.sink.formatwriter;

import java.io.OutputStream;

import com.microsoft.fabric.kafka.connect.sink.format.RecordWriter;
import com.microsoft.fabric.kafka.connect.sink.format.RecordWriterProvider;

public class KqlDbRecordWriterProvider implements RecordWriterProvider {
    @Override
    public RecordWriter getRecordWriter(String filename, OutputStream out) {
        return new KqlDbRecordWriter(filename, out);
    }
}
