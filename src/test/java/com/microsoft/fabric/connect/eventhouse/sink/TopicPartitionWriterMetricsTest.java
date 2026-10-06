package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.IngestionStatusResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.fabric.connect.eventhouse.sink.dlq.KafkaRecordErrorReporter;
import com.microsoft.fabric.connect.eventhouse.sink.dlq.NoOpLoggerErrorReporter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TopicPartitionWriterMetricsTest {
    private static final String DATABASE = "testdb1";
    private static final String TABLE = "testtable1";
    private static final TopicPartition TP = new TopicPartition("testPartition", 11);
    private static final HeaderTransforms NO_HEADER_TRANSFORMS = new HeaderTransforms(new HashSet<>(), new HashSet<>());

    private FabricSinkMetrics metrics;
    private File currentDirectory;

    @BeforeEach
    public void setUp() {
        metrics = new FabricSinkMetrics("tpw-metrics-test");
        currentDirectory = Utils.getCurrentWorkingDirectory();
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
        FileUtils.deleteQuietly(currentDirectory);
    }

    private FabricSinkConfig config(String behaviorOnError) {
        Map<String, String> settings = new HashMap<>();
        settings.put(FabricSinkConfig.KUSTO_INGEST_URL_CONF, "https://ingest-cluster.kusto.windows.net");
        settings.put(FabricSinkConfig.KUSTO_ENGINE_URL_CONF, "https://cluster.kusto.windows.net");
        settings.put(FabricSinkConfig.KUSTO_TABLES_MAPPING_CONF, "[{'topic': 'topic1', 'db': 'test', 'table': 'table1','format': 'csv'}]");
        settings.put(FabricSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(FabricSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(FabricSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        settings.put(FabricSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, Path.of(currentDirectory.getPath(), "testMetrics").toString());
        settings.put(FabricSinkConfig.KUSTO_SINK_FLUSH_SIZE_BYTES_CONF, "100000");
        settings.put(FabricSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, "60000");
        settings.put(FabricSinkConfig.KUSTO_BEHAVIOR_ON_ERROR_CONF, behaviorOnError);
        return new FabricSinkConfig(settings);
    }

    private static TopicIngestionProperties props() {
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        props.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
        return props;
    }

    @Test
    public void writeRecordShouldCountRecordsWritten() {
        TopicPartitionWriter writer = new TopicPartitionWriter(TP, mock(IngestClient.class), props(), config("FAIL"), false,
                Utils.noOpKafkaRecordErrorReporter(), metrics);
        writer.open();
        for (int i = 0; i < 5; i++) {
            writer.writeRecord(new SinkRecord(TP.topic(), TP.partition(), null, null, Schema.STRING_SCHEMA, "msg," + i, i),
                    NO_HEADER_TRANSFORMS);
        }
        writer.close();
        assertEquals(5, metrics.getRecordsWritten());
        assertEquals(0, metrics.getRecordsFailed());
    }

    @Test
    public void tombstoneRecordShouldStillBeCountedAsWritten() {
        TopicPartitionWriter writer = new TopicPartitionWriter(TP, mock(IngestClient.class), props(), config("FAIL"), false,
                Utils.noOpKafkaRecordErrorReporter(), metrics);
        writer.open();
        writer.writeRecord(new SinkRecord(TP.topic(), TP.partition(), Schema.STRING_SCHEMA, "key-1", null, null, 1),
                NO_HEADER_TRANSFORMS);
        writer.close();
        assertEquals(1, metrics.getRecordsWritten());
    }

    @Test
    public void successfulIngestionShouldCountAttemptAndSuccess() {
        TopicPartitionWriter writer = new TopicPartitionWriter(TP, mock(IngestClient.class), props(), config("FAIL"), false,
                Utils.noOpKafkaRecordErrorReporter(), metrics);
        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;
        writer.handleRollFile(descriptor);
        assertEquals(1, metrics.getIngestionAttempts());
        assertEquals(1, metrics.getIngestionSuccesses());
        assertEquals(0, metrics.getIngestionFailures());
    }

    @Test
    public void failedIngestionShouldCountFailureAndDlqRecords() {
        IngestClient client = mock(IngestClient.class);
        when(client.ingestFromFile(any(FileSourceInfo.class), any(IngestionProperties.class)))
                .thenThrow(new IngestionClientException("test failure"));
        KafkaRecordErrorReporter reporter = mock(KafkaRecordErrorReporter.class);
        TopicPartitionWriter writer = new TopicPartitionWriter(TP, client, props(), config("LOG"), true, reporter, metrics);
        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;
        descriptor.records.add(new SinkRecord(TP.topic(), TP.partition(), null, null, Schema.STRING_SCHEMA, "a", 1));
        descriptor.records.add(new SinkRecord(TP.topic(), TP.partition(), null, null, Schema.STRING_SCHEMA, "b", 2));
        writer.handleRollFile(descriptor);
        assertEquals(1, metrics.getIngestionAttempts());
        assertEquals(0, metrics.getIngestionSuccesses());
        assertEquals(1, metrics.getIngestionFailures());
        assertEquals(2, metrics.getDlqRecordsSent());
        verify(reporter, times(2)).reportError(any(SinkRecord.class), any(Exception.class));
    }

    @Test
    public void noOpReporterShouldNotCountDlqRecords() {
        IngestClient client = mock(IngestClient.class);
        when(client.ingestFromFile(any(FileSourceInfo.class), any(IngestionProperties.class)))
                .thenThrow(new IngestionClientException("test failure"));
        TopicPartitionWriter writer = new TopicPartitionWriter(TP, client, props(), config("LOG"), false,
                new NoOpLoggerErrorReporter(), metrics);
        SourceFile descriptor = new SourceFile();
        descriptor.records.add(new SinkRecord(TP.topic(), TP.partition(), null, null, Schema.STRING_SCHEMA, "a", 1));
        writer.handleRollFile(descriptor);
        assertEquals(1, metrics.getIngestionFailures());
        assertEquals(0, metrics.getDlqRecordsSent());
    }

    @Test
    public void failedStreamingStatusShouldCountAsFailureNotSuccess() {
        IngestClient client = mock(IngestClient.class);
        IngestionStatus failed = new IngestionStatus();
        failed.status = OperationStatus.Failed;
        when(client.ingestFromFile(any(FileSourceInfo.class), any(IngestionProperties.class)))
                .thenReturn(new IngestionStatusResult(failed));
        TopicIngestionProperties props = props();
        props.streaming = true;
        TopicPartitionWriter writer = new TopicPartitionWriter(TP, client, props, config("LOG"), false,
                Utils.noOpKafkaRecordErrorReporter(), metrics);
        writer.handleRollFile(new SourceFile());
        assertEquals(1, metrics.getIngestionAttempts());
        assertEquals(0, metrics.getIngestionSuccesses());
        assertEquals(1, metrics.getIngestionFailures());
    }

    @Test
    public void succeededStreamingStatusShouldCountAsSuccess() {
        IngestClient client = mock(IngestClient.class);
        IngestionStatus ok = new IngestionStatus();
        ok.status = OperationStatus.Succeeded;
        when(client.ingestFromFile(any(FileSourceInfo.class), any(IngestionProperties.class)))
                .thenReturn(new IngestionStatusResult(ok));
        TopicIngestionProperties props = props();
        props.streaming = true;
        TopicPartitionWriter writer = new TopicPartitionWriter(TP, client, props, config("LOG"), false,
                Utils.noOpKafkaRecordErrorReporter(), metrics);
        writer.handleRollFile(new SourceFile());
        assertEquals(1, metrics.getIngestionSuccesses());
        assertEquals(0, metrics.getIngestionFailures());
    }

    @Test
    public void writerWithoutMetricsShouldWork() {
        TopicPartitionWriter writer = new TopicPartitionWriter(TP, mock(IngestClient.class), props(), config("FAIL"), false,
                Utils.noOpKafkaRecordErrorReporter());
        writer.open();
        writer.writeRecord(new SinkRecord(TP.topic(), TP.partition(), null, null, Schema.STRING_SCHEMA, "msg", 1),
                NO_HEADER_TRANSFORMS);
        SourceFile descriptor = new SourceFile();
        writer.handleRollFile(descriptor);
        writer.close();
        assertEquals(0, metrics.getRecordsWritten());
    }
}
