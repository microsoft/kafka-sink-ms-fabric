package com.microsoft.fabric.connect.eventhouse.sink.format;

import java.io.OutputStream;

public interface RecordWriterProvider {
    RecordWriter getRecordWriter(String fileName, OutputStream out);
}
