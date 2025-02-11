package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.*;
import java.nio.file.Files;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConfig.BehaviorOnError;
import com.microsoft.fabric.connect.eventhouse.sink.format.RecordWriter;
import com.microsoft.fabric.connect.eventhouse.sink.format.RecordWriterProvider;
import com.microsoft.fabric.connect.eventhouse.sink.formatwriter.EventHouseRecordWriterProvider;
import com.microsoft.fabric.connect.eventhouse.sink.metrics.KustoKafkaMetricsUtil;

/**
 * This class is used to write gzipped rolling files.
 * Currently, supports size based rolling, where size is for *uncompressed* size,
 * so final size can vary.
 */
public class FileWriter implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileWriter.class);
    private final long flushInterval;
    private final IngestionProperties.DataFormat format;
    private final Consumer<SourceFile> onRollCallback;
    private final Function<Long, String> getFilePath;
    private final String basePath;
    private final long fileThreshold;
    // Lock is given from TopicPartitionWriter to lock while ingesting
    private final ReentrantReadWriteLock reentrantReadWriteLock;
    private final BehaviorOnError behaviorOnError;
    SourceFile currentFile;
    private Timer timer;
    private GZIPOutputStream outputStream;
    private CountingOutputStream countingStream;
    // Don't remove! File descriptor is kept so that the file is not deleted when stream is closed
    private FileDescriptor currentFileDescriptor;
    private String flushError;
    private RecordWriterProvider recordWriterProvider;
    private RecordWriter recordWriter;
    private boolean shouldWriteAvroAsBytes = false;
    private boolean stopped = false;
    private boolean isDlqEnabled = false;
    private FabricSinkConfig fabricSinkConfig;
    private Counter fileCountOnStage;
    private Counter fileCountPurged;
    private Counter bufferSizeBytes;
    private Counter bufferRecordCount;
    private long flushedOffsetValue;
    private long purgedOffsetValue;
    private Counter failedTempFileDeletions;
    

    /**
     *
     * @param basePath The base path for the files
     * @param fileThreshold The threshold for the file size
     * @param onRollCallback The callback to be called when the file is rolled
     * @param getFilePath The function to get the file path
     * @param flushInterval The interval to flush the file
     * @param reentrantLock The lock to be used for the file writer
     * @param format The format of the data
     * @param behaviorOnError The behavior on error
     * @param isDlqEnabled The flag to enable DLQ
     * @param fabricSinkConfig The fabric sink config
     */
    public FileWriter(String basePath,
            long fileThreshold,
            Consumer<SourceFile> onRollCallback,
            Function<Long, String> getFilePath,
            long flushInterval,
            ReentrantReadWriteLock reentrantLock,
            IngestionProperties.DataFormat format,
            BehaviorOnError behaviorOnError,
            boolean isDlqEnabled, FabricSinkConfig fabricSinkConfig,
            String tpname,
            MetricRegistry metricRegistry) {
        this.getFilePath = getFilePath;
        this.basePath = basePath;
        this.fileThreshold = fileThreshold;
        this.onRollCallback = onRollCallback;
        this.flushInterval = flushInterval;
        this.behaviorOnError = behaviorOnError;
        this.isDlqEnabled = isDlqEnabled;
        // This is a fair lock so that we flush close to the time intervals
        this.reentrantReadWriteLock = reentrantLock;
        // Initialize the format field
        this.format = format;
        // If we failed on flush we want to throw the error from the put() flow.
        flushError = null;
        this.fabricSinkConfig = fabricSinkConfig;
        initializeMetrics(tpname, metricRegistry);
    }

    private void initializeMetrics(String tpname, MetricRegistry metricRegistry) {
        this.fileCountOnStage = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(tpname, KustoKafkaMetricsUtil.FILE_COUNT_SUB_DOMAIN, KustoKafkaMetricsUtil.FILE_COUNT_ON_STAGE));
        this.fileCountPurged = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(tpname, KustoKafkaMetricsUtil.FILE_COUNT_SUB_DOMAIN, KustoKafkaMetricsUtil.FILE_COUNT_PURGED));
        this.bufferSizeBytes = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(tpname, KustoKafkaMetricsUtil.BUFFER_SUB_DOMAIN, KustoKafkaMetricsUtil.BUFFER_SIZE_BYTES));
        this.bufferRecordCount = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(tpname, KustoKafkaMetricsUtil.BUFFER_SUB_DOMAIN, KustoKafkaMetricsUtil.BUFFER_RECORD_COUNT));
        this.failedTempFileDeletions = metricRegistry.counter(KustoKafkaMetricsUtil.constructMetricName(tpname, KustoKafkaMetricsUtil.FILE_COUNT_SUB_DOMAIN, KustoKafkaMetricsUtil.FAILED_TEMP_FILE_DELETIONS));
        String flushedOffsetMetricName = KustoKafkaMetricsUtil.constructMetricName(tpname, KustoKafkaMetricsUtil.OFFSET_SUB_DOMAIN, KustoKafkaMetricsUtil.FLUSHED_OFFSET);
        if (!metricRegistry.getGauges().containsKey(flushedOffsetMetricName)) {
            metricRegistry.register(flushedOffsetMetricName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return flushedOffsetValue;
                }
            });
        }
    
        String purgedOffsetMetricName = KustoKafkaMetricsUtil.constructMetricName(tpname, KustoKafkaMetricsUtil.OFFSET_SUB_DOMAIN, KustoKafkaMetricsUtil.PURGED_OFFSET);
        if (!metricRegistry.getGauges().containsKey(purgedOffsetMetricName)) {
            metricRegistry.register(purgedOffsetMetricName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return purgedOffsetValue;
                }
            });
        }
    }

    boolean isDirty() {
        return this.currentFile != null && this.currentFile.rawBytes > 0;
    }

    public void openFile(@Nullable Long offset) throws IOException {
        SourceFile fileProps = new SourceFile();
        File folder = new File(basePath);
        if (!folder.exists() && !folder.mkdirs()) {
            if (!folder.exists()) {
                throw new IOException(String.format("Failed to create new directory %s", folder.getPath()));
            }
            LOGGER.warn("Couldn't create the directory because it already exists (likely a race condition)");
        }
        String filePath = getFilePath.apply(offset);
        fileProps.path = filePath;
        // Sanitize the file name just be sure and make sure it has the R/W permissions only
        String sanitizedFilePath = FilenameUtils.normalize(filePath);
        if (sanitizedFilePath == null) {
            /*
             * This condition should not occur at all. The files are created in controlled manner with the names consisting DB name, table name. This does not
             * permit names like "../../" or "./" etc. Adding an additional check.
             */
            String errorMessage = String.format("Exception creating local file for write." +
                    "File %s has a non canonical path", filePath);
            throw new ConnectException(errorMessage);
        }
        File file = new File(sanitizedFilePath);
        boolean createFile = file.createNewFile(); // if there is a runtime exception. It gets thrown from here
        if (createFile) {
            /*
             * Setting restricted permissions on the file. If these permissions cannot be set, then warn - We cannot fail the ingestion (Failing the ingestion
             * would for not having the permission would mean that there may be data loss or unexpected scenarios.) Added this in a conditional as these
             * permissions can be applied only when the file is created
             */
            try {
                boolean execResult = file.setReadable(true, true);
                execResult = execResult && file.setWritable(true, true);
                execResult = execResult && file.setExecutable(false, false);
                if (!execResult) {
                    LOGGER.warn("Setting permissions creating file {} returned false." +
                            "The files set for ingestion can be read by other applications having access." +
                            "Please check security policies on the host that is preventing file permissions from being applied",
                            filePath);
                }
            } catch (Exception ex) {
                // There is a likely chance of the permissions not getting set. This is set to warn
                LOGGER.warn("Exception permissions creating file {} returned false." +
                        "The files set for ingestion can be read by other applications having access." +
                        "Please check security policies on the host that is preventing file permissions being applied",
                        filePath, ex);

            }
        }
        // The underlying file is closed only when the current countingStream (abstraction for size based writes) and
        // the file is rolled over
        FileOutputStream fos = new FileOutputStream(file);
        currentFileDescriptor = fos.getFD();
        fos.getChannel().truncate(0);
        fileProps.file = file;
        currentFile = fileProps;
        countingStream = new CountingOutputStream(new GZIPOutputStream(fos));
        outputStream = countingStream.getOutputStream();
        LOGGER.debug("Opened new file for writing: {}", fileProps.file);
        recordWriter = recordWriterProvider.getRecordWriter(currentFile.path, countingStream, fabricSinkConfig);
        fileCountOnStage.inc(); // Increment the file count on stage counter
    }

    void rotate(@Nullable Long offset) throws IOException, DataException {
        finishFile();
        openFile(offset);
    }

    void finishFile() throws IOException, DataException {
        if (isDirty()) {
            recordWriter.commit();
            // Since we are using GZIP compression, finish the file. Close is invoked only when this flush finishes
            // and then the file is finished in ingest
            // This is called when there is a time or a size limit reached. The file is then reset/rolled and then a
            // new file is created for processing
            outputStream.finish();
            // It could be we were waiting on the lock when task suddenly stops , we should not ingest anymore
            if (stopped) {
                return;
            }
            try {
                onRollCallback.accept(currentFile);
            } catch (ConnectException e) {
                /*
                 * Swallow the exception and continue to process subsequent records when behavior.on.error is not set to fail mode. Also, throwing/logging the
                 * exception with just a message to avoid polluting logs with duplicate trace.
                 */
                handleErrors(e);
            }
            dumpFile();
        } else {
            // The stream is closed only when there are non-empty files for ingestion. Note that this closes the
            // FileOutputStream as well
            outputStream.close();
            currentFile = null;
        }
    }

    private void handleErrors(Exception e) {
        String errorMessage = "Failed to write records to KustoDB.";
        if (FabricSinkConfig.BehaviorOnError.FAIL == behaviorOnError) {
            throw new ConnectException(errorMessage, e);
        } else if (FabricSinkConfig.BehaviorOnError.LOG == behaviorOnError) {
            LOGGER.error("{}", errorMessage, e);
        } else {
            LOGGER.debug("{}", errorMessage, e);
        }
    }

    private void dumpFile() throws IOException {
        SourceFile temp = currentFile;
        currentFile = null;
        if (temp != null) {
            countingStream.close();
            currentFileDescriptor = null;
            boolean deleted = Files.deleteIfExists(temp.file.toPath());
            if (!deleted) {
                LOGGER.warn("Couldn't delete temporary file. File exists: {}", temp.file.exists());
                failedTempFileDeletions.inc();
            } else {
                fileCountPurged.inc();
                purgedOffsetValue = flushedOffsetValue; // Update purgedOffsetValue
                fileCountOnStage.dec();
            }
        }
    }

    public synchronized void rollback() throws IOException {
        if (countingStream != null) {
            countingStream.close();
            if (currentFile != null && currentFile.file != null) {
                dumpFile();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        stop();
    }

    public synchronized void stop() throws DataException {
        stopped = true;
        if (timer != null) {
            Timer temp = timer;
            timer = null;
            temp.cancel();
        }
    }

    // Set shouldDestroyTimer to true if the current running task should be cancelled
    private void resetFlushTimer(boolean shouldDestroyTimer) {
        if (flushInterval > 0) {
            if (shouldDestroyTimer) {
                if (timer != null) {
                    timer.cancel();
                }
                timer = new Timer(true);
            }
            TimerTask t = new TimerTask() {
                @Override
                public void run() {
                    flushByTimeImpl();
                }
            };
            if (timer != null) {
                timer.schedule(t, flushInterval);
            }
        }
    }

    void flushByTimeImpl() {
        // Flush time interval gets the write lock so that it won't starve
        try (AutoCloseableLock ignored = new AutoCloseableLock(reentrantReadWriteLock.writeLock())) {
            if (stopped) {
                return;
            }
            // Lock before the check so that if a writing process just flushed this won't ingest empty files
            if (isDirty()) {
                finishFile();
            }
            resetFlushTimer(false);
        } catch (Exception e) {
            String fileName = currentFile == null ? "[no file created yet]" : currentFile.file.getName();
            long currentSize = currentFile == null ? 0 : currentFile.rawBytes;
            flushError = String.format("Error in flushByTime. Current file: %s, size: %d. ", fileName, currentSize);
            LOGGER.error(flushError, e);
        }
    }

    public void writeData(SinkRecord sinkRecord, HeaderTransforms headerTransforms) throws IOException, DataException {
        if (flushError != null) {
            throw new ConnectException(flushError);
        }
        if (sinkRecord == null)
            return;
        if (recordWriterProvider == null) {
            initializeRecordWriter(sinkRecord);
        }
        if (currentFile == null) {
            openFile(sinkRecord.kafkaOffset());
            resetFlushTimer(true);
        }
        recordWriter.write(sinkRecord, this.format, headerTransforms);
        if (this.isDlqEnabled) {
            currentFile.records.add(sinkRecord);
        }
        currentFile.rawBytes = countingStream.numBytes;
        currentFile.numRecords++;
        synchronized (bufferSizeBytes) {
            bufferSizeBytes.dec(bufferSizeBytes.getCount()); // Reset the counter to zero
            bufferSizeBytes.inc(currentFile.rawBytes); // Set the counter to the current size
        }
        synchronized (bufferRecordCount) {
            bufferRecordCount.dec(bufferRecordCount.getCount()); // Reset the counter to zero
            bufferRecordCount.inc(currentFile.numRecords); // Set the counter to the current number of records
        }
        flushedOffsetValue = sinkRecord.kafkaOffset(); // Update flushedOffsetValue
    
        if (this.flushInterval == 0 || currentFile.rawBytes > fileThreshold || shouldWriteAvroAsBytes) {
            rotate(sinkRecord.kafkaOffset());
            resetFlushTimer(true);
        }
    }

    public void initializeRecordWriter(@NotNull SinkRecord sinkRecord) {
        if (sinkRecord.value() instanceof Map) {
            recordWriterProvider = new EventHouseRecordWriterProvider();
        } else if ((sinkRecord.valueSchema() != null) && (sinkRecord.valueSchema().type() == Schema.Type.STRUCT)) {
            if (format.equals(IngestionProperties.DataFormat.JSON) || format.equals(IngestionProperties.DataFormat.MULTIJSON)) {
                recordWriterProvider = new EventHouseRecordWriterProvider();
            } else if (format.equals(IngestionProperties.DataFormat.AVRO)) {
                recordWriterProvider = new EventHouseRecordWriterProvider();
            } else {
                throw new ConnectException(String.format("Invalid Kusto table mapping, Kafka records of type "
                        + "Avro and JSON can only be ingested to Kusto table having Avro or JSON mapping. "
                        + "Currently, it is of type %s.", format));
            }
        } else if ((sinkRecord.valueSchema() == null) || (sinkRecord.valueSchema().type() == Schema.Type.STRING)) {
            recordWriterProvider = new EventHouseRecordWriterProvider();
        } else if ((sinkRecord.valueSchema() != null) && (sinkRecord.valueSchema().type() == Schema.Type.BYTES)) {
            recordWriterProvider = new EventHouseRecordWriterProvider();
            if (format.equals(IngestionProperties.DataFormat.AVRO)) {
                shouldWriteAvroAsBytes = true;
            }
        } else {
            throw new ConnectException(String.format(
                    "Invalid Kafka record format, connector does not support %s format. This connector supports Avro, Json with schema, Json without schema, Byte, String format. ",
                    sinkRecord.valueSchema().type()));
        }
    }

    private static class CountingOutputStream extends FilterOutputStream {
        private final GZIPOutputStream outputStream;
        private long numBytes = 0;

        CountingOutputStream(GZIPOutputStream out) {
            super(out);
            this.outputStream = out;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            this.numBytes++;
        }

        @Override
        public void write(byte @NotNull [] b) throws IOException {
            out.write(b);
            this.numBytes += b.length;
        }

        @Override
        public void write(byte @NotNull [] b, int off, int len) throws IOException {
            out.write(b, off, len);
            this.numBytes += len;
        }

        public GZIPOutputStream getOutputStream() {
            return this.outputStream;
        }
    }
}
