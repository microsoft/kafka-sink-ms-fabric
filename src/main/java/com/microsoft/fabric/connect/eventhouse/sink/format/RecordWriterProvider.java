package com.microsoft.fabric.connect.eventhouse.sink.format;

import java.io.OutputStream;

import com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConfig;

public interface RecordWriterProvider {
    RecordWriter getRecordWriter(String fileName, OutputStream out, FabricSinkConfig fabricSinkConfig);
}
