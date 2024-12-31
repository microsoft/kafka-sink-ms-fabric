package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

public class SourceFile {
    String path;
    File file;
    final List<SinkRecord> records = new ArrayList<>();
    long rawBytes = 0;
    long numRecords = 0;
}
