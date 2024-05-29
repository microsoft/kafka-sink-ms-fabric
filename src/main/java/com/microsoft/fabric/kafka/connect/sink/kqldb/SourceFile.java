package com.microsoft.fabric.kafka.connect.sink.kqldb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

public class SourceFile {
    public String path;
    public File file;
    public List<SinkRecord> records = new ArrayList<>();
    long rawBytes = 0;
    long numRecords = 0;
}
