package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

public class TopicPartitionWriterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionWriterTest.class);

    private static final String KUSTO_INGEST_CLUSTER_URL = "https://ingest-cluster.kusto.windows.net";
    private static final String KUSTO_CLUSTER_URL = "https://cluster.kusto.windows.net";
    private static final String DATABASE = "testdb1";
    private static final String TABLE = "testtable1";
    private static final long FILE_THRESHOLD = 100;
    private static final long FLUSH_INTERVAL = 5000;
    private static final IngestClient mockClient = mock(IngestClient.class);
    private static final TopicIngestionProperties propsCsv = new TopicIngestionProperties();
    private static final TopicPartition tp = new TopicPartition("testPartition", 11);
    private static final long CONTEXT_SWITCH_INTERVAL = 200;
    private static FabricSinkConfig config;
    private File currentDirectory;
    private String basePathCurrent;
    private boolean isDlqEnabled;
    private String dlqTopicName;
    private Producer<byte[], byte[]> kafkaProducer;

    @BeforeAll
    public static void beforeClass() {
        propsCsv.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        propsCsv.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
    }

    @BeforeEach
    public final void before() {
        currentDirectory = Utils.getCurrentWorkingDirectory();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9000");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducer = new KafkaProducer<>(properties);
        isDlqEnabled = false;
        dlqTopicName = null;
        basePathCurrent = Paths.get(currentDirectory.getPath(), "testWriteStringyValuesAndOffset").toString();
        Map<String, String> settings = getKustoConfigs(basePathCurrent, FILE_THRESHOLD, FLUSH_INTERVAL);
        config = new FabricSinkConfig(settings);
    }

    @AfterEach
    public final void afterEach() {
        FileUtils.deleteQuietly(currentDirectory);
    }

    @Test
    void  testHandleRollFile() {
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, config, isDlqEnabled, dlqTopicName, kafkaProducer);
        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;
        writer.handleRollFile(descriptor);
        ArgumentCaptor<FileSourceInfo> fileSourceInfoArgument = ArgumentCaptor.forClass(FileSourceInfo.class);
        ArgumentCaptor<IngestionProperties> ingestionPropertiesArgumentCaptor = ArgumentCaptor.forClass(IngestionProperties.class);
        try {
            verify(mockedClient, only()).ingestFromFile(fileSourceInfoArgument.capture(), ingestionPropertiesArgumentCaptor.capture());
        } catch (Exception e) {
            LOGGER.error("Error running testHandleRollFile", e);
            fail(e);
        }

        Assertions.assertEquals(fileSourceInfoArgument.getValue().getFilePath(), descriptor.path);
        Assertions.assertEquals(TABLE, ingestionPropertiesArgumentCaptor.getValue().getTableName());
        Assertions.assertEquals(DATABASE, ingestionPropertiesArgumentCaptor.getValue().getDatabaseName());
        Assertions.assertEquals(1024, fileSourceInfoArgument.getValue().getRawSizeInBytes());
    }

    @Test
    void  testHandleRollFileWithStreamingEnabled() {
        IngestClient mockedClient = mock(IngestClient.class);
        TopicIngestionProperties props = new TopicIngestionProperties();
        props.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        props.streaming = true;
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockedClient, props, config, isDlqEnabled, dlqTopicName, kafkaProducer);

        SourceFile descriptor = new SourceFile();
        descriptor.rawBytes = 1024;

        writer.handleRollFile(descriptor);
        ArgumentCaptor<FileSourceInfo> fileSourceInfoArgument = ArgumentCaptor.forClass(FileSourceInfo.class);
        ArgumentCaptor<IngestionProperties> ingestionPropertiesArgumentCaptor = ArgumentCaptor.forClass(IngestionProperties.class);
        try {
            verify(mockedClient, only()).ingestFromFile(fileSourceInfoArgument.capture(), ingestionPropertiesArgumentCaptor.capture());
        } catch (Exception e) {
            LOGGER.error("Error running testHandleRollFile", e);
            fail(e);
        }

        Assertions.assertEquals(fileSourceInfoArgument.getValue().getFilePath(), descriptor.path);
        Assertions.assertEquals(TABLE, ingestionPropertiesArgumentCaptor.getValue().getTableName());
        Assertions.assertEquals(DATABASE, ingestionPropertiesArgumentCaptor.getValue().getDatabaseName());
        Assertions.assertEquals(1024, fileSourceInfoArgument.getValue().getRawSizeInBytes());
    }

    @Test
    void  testGetFilename() {
        try {
            TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);
            File writerFile = new File(writer.getFilePath(null));
            Assertions.assertEquals("kafka_testPartition_11_0.JSON.gz", writerFile.getName());
        } catch (Exception ex) {
            // In case there is an accessor exception getting the file
            fail(ex);
        }
    }

    @Contract(" -> new")
    private @NotNull HeaderTransforms getHeaderTransforms() {
        Set<String> headersToDrop = new HashSet<>();
        headersToDrop.add("dropHeader1");
        headersToDrop.add("dropHeader2");
        Set<String> headersToProject = new HashSet<>();
        headersToProject.add("projectHeader1");
        headersToProject.add("projectHeader2");
        return new HeaderTransforms(headersToDrop, headersToProject);
    }

    @Test
    void  testGetFilenameAfterOffsetChanges() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);
        writer.open();
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 5));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));
        for (SinkRecord sinkRecord  : records) {
            writer.writeRecord(sinkRecord,getHeaderTransforms());
        }
        try {
            File writerFile = new File(writer.getFilePath(null));
            Assertions.assertTrue(writerFile.exists());
            Assertions.assertEquals("kafka_testPartition_11_4.JSON.gz", (new File(writer.getFilePath(null))).getName());
        } catch (Exception ex) {
            // In case there is an accessor exception getting the file
            fail(ex);
        }
    }

    @Test
    void  testWriteStringyValuesAndOffset() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);
        writer.open();
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 3));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));
        for (SinkRecord sinkRecord : records) {
            writer.writeRecord(sinkRecord,getHeaderTransforms());
        }
        Assertions.assertTrue((new File(writer.fileWriter.currentFile.path)).exists());
        Assertions.assertEquals(String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(), 4,
                IngestionProperties.DataFormat.JSON.name()),
                (new File(writer.fileWriter.currentFile.path)).getName());
        writer.close();
    }

    @Test
    void  testWriteBytesValuesAndOffset() throws IOException {
        byte[] message = IOUtils.toByteArray(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("data.avro")));
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(message);
        // Expect to finish file with one record although fileThreshold is high
        long fileThreshold2 = 128;
        TopicIngestionProperties propsAvro = new TopicIngestionProperties();
        propsAvro.ingestionProperties = new IngestionProperties(DATABASE, TABLE);
        propsAvro.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
        Map<String, String> settings2 = getKustoConfigs(basePathCurrent, fileThreshold2, FLUSH_INTERVAL);
        FabricSinkConfig config2 = new FabricSinkConfig(settings2);
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsAvro, config2, isDlqEnabled, dlqTopicName, kafkaProducer);

        writer.open();
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.BYTES_SCHEMA, o.toByteArray(), 10));

        for (SinkRecord sinkRecord : records) {
            writer.writeRecord(sinkRecord,getHeaderTransforms());
        }

        Assertions.assertEquals(10, (long) writer.lastCommittedOffset);
        Assertions.assertEquals(10, writer.currentOffset);

        String currentFileName = writer.fileWriter.currentFile.path;

        Assertions.assertTrue(new File(currentFileName).exists());
        Assertions.assertEquals(String.format("kafka_%s_%d_%d.%s.gz", tp.topic(), tp.partition(),
                10, IngestionProperties.DataFormat.AVRO.name()),
                (new File(currentFileName)).getName());
        writer.close();
    }

    @Test
    void  testClose() {
        TopicPartitionWriter writer = new TopicPartitionWriter(tp, mockClient, propsCsv, config, isDlqEnabled, dlqTopicName, kafkaProducer);
        TopicPartitionWriter spyWriter = spy(writer);

        spyWriter.open();
        List<SinkRecord> records = new ArrayList<>();

        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "another,stringy,message", 5));
        records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, Schema.STRING_SCHEMA, "{'also':'stringy','sortof':'message'}", 4));

        for (SinkRecord sinkRecord  : records) {
            spyWriter.writeRecord(sinkRecord,getHeaderTransforms());
        }
        // 2 records are waiting to be ingested - expect close to revoke them so that even after 5 seconds it won't ingest
        Assertions.assertNotNull(spyWriter.lastCommittedOffset);
        spyWriter.close();
        Awaitility.await().atMost(FLUSH_INTERVAL + CONTEXT_SWITCH_INTERVAL, TimeUnit.MILLISECONDS).
                until(()->spyWriter.lastCommittedOffset!=null);
    }

    private @NotNull Map<String, String> getKustoConfigs(String basePath, long fileThreshold, long flushInterval) {
        Map<String, String> settings = new HashMap<>();
        settings.put(FabricSinkConfig.KUSTO_INGEST_URL_CONF, KUSTO_INGEST_CLUSTER_URL);
        settings.put(FabricSinkConfig.KUSTO_ENGINE_URL_CONF, KUSTO_CLUSTER_URL);
        settings.put(FabricSinkConfig.KUSTO_TABLES_MAPPING_CONF, "mapping");
        settings.put(FabricSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        settings.put(FabricSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        settings.put(FabricSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        settings.put(FabricSinkConfig.KUSTO_SINK_TEMP_DIR_CONF, basePath);
        settings.put(FabricSinkConfig.KUSTO_SINK_FLUSH_SIZE_BYTES_CONF, String.valueOf(fileThreshold));
        settings.put(FabricSinkConfig.KUSTO_SINK_FLUSH_INTERVAL_MS_CONF, String.valueOf(flushInterval));
        return settings;
    }
}
