package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConfig.BehaviorOnError;
import com.microsoft.fabric.connect.eventhouse.sink.formatwriter.EventHouseRecordWriter;

import static com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConnectorConfigTest.setupConfigs;
import static com.microsoft.fabric.connect.eventhouse.sink.Utils.createDirectoryWithPermissions;
import static com.microsoft.fabric.connect.eventhouse.sink.Utils.getFilesCount;
import static java.util.concurrent.TimeUnit.SECONDS;

class FileWriterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileWriterTest.class);
    IngestionProperties ingestionProps;
    private File currentDirectory;
    protected static final FabricSinkConfig FABRIC_SINK_CONFIG = new FabricSinkConfig(setupConfigs());

    @Contract(pure = true)
    static @NotNull Function<SourceFile, String> getAssertFileConsumerFunction(String msg) {
        return (SourceFile f) -> {
            try (FileInputStream fileInputStream = new FileInputStream(f.file)) {
                byte[] bytes = IOUtils.toByteArray(fileInputStream);
                try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
                        GZIPInputStream gzipper = new GZIPInputStream(bin)) {

                    byte[] buffer = new byte[1024];
                    ByteArrayOutputStream out = new ByteArrayOutputStream();

                    int len;
                    while ((len = gzipper.read(buffer)) > 0) {
                        out.write(buffer, 0, len);
                    }

                    gzipper.close();
                    out.close();
                    String s = out.toString();

                    Assertions.assertEquals(s, msg);
                }
            } catch (IOException e) {
                LOGGER.error("Error running test", e);
                Assertions.fail(e.getMessage());
            }
            return null;
        };
    }

    @BeforeEach
    public final void before() {
        currentDirectory = Utils.getCurrentWorkingDirectory();
        ingestionProps = new IngestionProperties("db", "table");
        ingestionProps.setDataFormat(IngestionProperties.DataFormat.CSV);
    }

    @AfterEach
    public final void afterEach() {
        FileUtils.deleteQuietly(currentDirectory);
    }

    @Test
    void testOpen() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testWriterOpen").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path));
        Assertions.assertEquals(0, getFilesCount(path));
        final String FILE_PATH = Paths.get(path, "ABC").toString();
        final int MAX_FILE_SIZE = 128;
        Consumer<SourceFile> trackFiles = (SourceFile f) -> {
        };
        Function<Long, String> generateFileName = (Long l) -> FILE_PATH;
        try (FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true, FABRIC_SINK_CONFIG)) {
            String msg = "Line number 1: This is a message from the other size";
            SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, msg.getBytes(), 10);
            fileWriter.initializeRecordWriter(sinkRecord);
            fileWriter.openFile(null);
            Assertions.assertEquals(1, getFilesCount(path));
            Assertions.assertEquals(0, fileWriter.currentFile.rawBytes);
            Assertions.assertEquals(FILE_PATH, fileWriter.currentFile.path);
            Assertions.assertTrue(fileWriter.currentFile.file.canWrite());
            fileWriter.rollback();
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
    void testGzipFileWriter() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path));
        Assertions.assertEquals(0, getFilesCount(path));
        HashMap<String, Long> files = new HashMap<>();
        final int MAX_FILE_SIZE = 225; // sizeof(,'','','{"partition":"1","offset":"1","topic":"topic"}'\n) * 2 , Similar multiple applied for the first test
        Consumer<SourceFile> trackFiles = (SourceFile f) -> files.put(f.path, f.rawBytes);
        Function<Long, String> generateFileName = (Long l) -> Paths.get(path, String.valueOf(java.util.UUID.randomUUID())) + "csv.gz";
        EventHouseRecordWriter eventHouseRecordWriter = new EventHouseRecordWriter(path, NullOutputStream.INSTANCE, FABRIC_SINK_CONFIG);
        try (FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000,
                new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true, FABRIC_SINK_CONFIG)) {
            for (int i = 0; i < 9; i++) {
                String msg = String.format("Line number %d : This is a message from the other size", i);
                SinkRecord record1 = new SinkRecord("topic", 1, null, null,
                        Schema.BYTES_SCHEMA, msg.getBytes(), 10);
                record1.headers().addString("projectHeader1", "projectHeaderValue1");
                record1.headers().addString("projectHeader2", "projectHeaderValue2");
                record1.headers().addString("dropHeader1", "someValue");
                record1.headers().addString("dropHeader2", "someValue");
                fileWriter.writeData(record1, getHeaderTransforms());
                Map<String, Object> headerResult = eventHouseRecordWriter.getHeadersAsMap(record1, getHeaderTransforms());
                Assertions.assertEquals(2, headerResult.size());
                Assertions.assertEquals("projectHeaderValue1", headerResult.get("projectHeader1"));
                Assertions.assertEquals("projectHeaderValue2", headerResult.get("projectHeader2"));
            }
            Assertions.assertEquals(4, files.size());
            // should still have 1 open file at this point...
            Assertions.assertEquals(1, getFilesCount(path));
            // close current file
            fileWriter.rotate(54L);
            Assertions.assertEquals(5, files.size());
            List<Long> sortedFiles = new ArrayList<>(files.values());
            sortedFiles.sort((Long x, Long y) -> (int) (y - x));
            Assertions.assertEquals(
                    Arrays.asList((long) 414, (long) 414, (long) 414, (long) 414, (long) 207),
                    sortedFiles);
            // make sure folder is clear once done - with only the new file
            Assertions.assertEquals(1, getFilesCount(path));

        }
    }

    @Test
    void testGzipFileWriterFlush() throws IOException {
        String path = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path));
        HashMap<String, Long> files = new HashMap<>();
        final int MAX_FILE_SIZE = 128 * 2;
        Consumer<SourceFile> trackFiles = (SourceFile f) -> files.put(f.path, f.rawBytes);
        Function<Long, String> generateFileName = (Long l) -> Paths.get(path, java.util.UUID.randomUUID().toString()) + "csv.gz";
        // Expect no files to be ingested as size is small and flushInterval is big
        FileWriter fileWriter = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 30000, new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true, FABRIC_SINK_CONFIG);
        String msg = "Message";
        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, null, msg, 10);
        fileWriter.writeData(sinkRecord, getHeaderTransforms());
        Awaitility.await().atMost(3, SECONDS).untilAsserted(() -> Assertions.assertEquals(0, files.size()));
        fileWriter.rotate(10L);
        fileWriter.stop();
        Assertions.assertEquals(1, files.size());

        String path2 = Paths.get(currentDirectory.getPath(), "testGzipFileWriter2_2").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path2));
        Function<Long, String> generateFileName2 = (Long l) -> Paths.get(path2, java.util.UUID.randomUUID().toString()).toString();
        // Expect one file to be ingested as flushInterval had changed and is shorter than sleep time
        FileWriter fileWriter2 = new FileWriter(path2, MAX_FILE_SIZE, trackFiles, generateFileName2, 1000, new ReentrantReadWriteLock(),
                ingestionProps.getDataFormat(), BehaviorOnError.FAIL, true, FABRIC_SINK_CONFIG);
        String msg2 = "Second Message";
        SinkRecord record1 = new SinkRecord("topic", 1, null, null, null, msg2, 10);
        fileWriter2.writeData(record1, getHeaderTransforms());
        Awaitility.await().atMost(3, SECONDS).untilAsserted(() -> Assertions.assertEquals(2, files.size()));
        List<Long> sortedFiles = new ArrayList<>(files.values());
        sortedFiles.sort((Long x, Long y) -> (int) (y - x));
        Assertions.assertEquals(sortedFiles, Arrays.asList((long) 81, (long) 74));
        // make sure folder is clear once done
        fileWriter2.close();
        Assertions.assertEquals(1, getFilesCount(path));
    }

    @Test
    void offsetCheckByInterval() throws InterruptedException, IOException {
        // This test will check that lastCommitOffset is set to the right value, when ingests are done by flush interval.
        // There will be a write operation followed by a flush which will track files and sleep.
        // While it sleeps there will be another write attempt which should wait on the lock and another flush later.
        // Resulting in first record to be with offset 1 and second with offset 2.

        ArrayList<Map.Entry<String, Long>> files = new ArrayList<>();
        final int MAX_FILE_SIZE = 128 * 2;
        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        final ArrayList<Long> committedOffsets = new ArrayList<>();
        class Offsets {
            private long currentOffset = 0;
        }
        final Offsets offsets = new Offsets();
        Consumer<SourceFile> trackFiles = (SourceFile f) -> {
            committedOffsets.add(offsets.currentOffset);
            files.add(new AbstractMap.SimpleEntry<>(f.path, f.rawBytes));
        };
        String path = Paths.get(currentDirectory.getPath(), "offsetCheckByInterval").toString();
        Assertions.assertTrue(createDirectoryWithPermissions(path));
        Function<Long, String> generateFileName = (Long offset) -> {
            if (offset == null) {
                offset = offsets.currentOffset;
            }
            return Paths.get(path, Long.toString(offset)).toString();
        };
        try (FileWriter fileWriter2 = new FileWriter(path, MAX_FILE_SIZE, trackFiles, generateFileName, 500,
                reentrantReadWriteLock,
                ingestionProps.getDataFormat(),
                BehaviorOnError.FAIL, true, FABRIC_SINK_CONFIG)) {
            String msg2 = "Second Message";
            reentrantReadWriteLock.readLock().lock();
            long recordOffset = 1;
            SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);
            fileWriter2.writeData(sinkRecord, getHeaderTransforms());
            offsets.currentOffset = recordOffset;
            // Wake the flush by interval in the middle of the writing
            Thread.sleep(510);
            recordOffset = 2;
            SinkRecord record2 = new SinkRecord("TestTopic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);

            fileWriter2.writeData(record2, getHeaderTransforms());
            offsets.currentOffset = recordOffset;
            reentrantReadWriteLock.readLock().unlock();

            // Context switch
            reentrantReadWriteLock.readLock().lock();
            recordOffset = 3;
            SinkRecord record3 = new SinkRecord("TestTopic", 1, null, null, Schema.BYTES_SCHEMA, msg2.getBytes(), recordOffset);

            offsets.currentOffset = recordOffset;
            fileWriter2.writeData(record3, getHeaderTransforms());
            reentrantReadWriteLock.readLock().unlock();
            // Assertions
            Awaitility.await().atMost(3, SECONDS).untilAsserted(() -> Assertions.assertEquals(2, files.size()));

            // Make sure that the first file is from offset 1 till 2 and second is from 3 till 3
            /*
             * > Why is this 30 before ? 2 * "Second Message" + NewLines for both 6(Second)+1(Space)+7(Message)+1(New Line) -> 15 2 of these => 30
             * 
             * > Why did this become 146 ? The CSV now becomes : 'Second Message','','','{"partition":"1","offset":"1","topic":"topic"}'\n 2 of these become 146
             * bytes
             */
            Assertions.assertEquals(164L, files.stream().map(Map.Entry::getValue).toArray(Long[]::new)[0]);
            Assertions.assertEquals(84L, files.stream().map(Map.Entry::getValue).toArray(Long[]::new)[1]);
            Assertions.assertEquals("1",
                    files.stream().map(s -> s.getKey().substring(path.length() + 1)).toArray(String[]::new)[0]);
            Assertions.assertEquals("3",
                    files.stream().map(s -> s.getKey().substring(path.length() + 1)).toArray(String[]::new)[1]);
            Assertions.assertEquals(committedOffsets, new ArrayList<Long>() {
                {
                    add(2L);
                    add(3L);
                }
            });
            // make sure folder is clear once done
            fileWriter2.stop();
            Assertions.assertEquals(0, getFilesCount(path));
        }
    }
}
