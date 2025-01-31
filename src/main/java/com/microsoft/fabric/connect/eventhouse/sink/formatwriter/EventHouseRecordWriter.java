package com.microsoft.fabric.connect.eventhouse.sink.formatwriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConfig;
import com.microsoft.fabric.connect.eventhouse.sink.HeaderTransforms;
import com.microsoft.fabric.connect.eventhouse.sink.format.RecordWriter;

public class EventHouseRecordWriter extends HeaderAndMetadataWriter implements RecordWriter {
    private final String filename;
    private final JsonGenerator writer;
    private final OutputStream plainOutputStream;
    private Schema schema;
    private final FabricSinkConfig fabricSinkConfig;

    public EventHouseRecordWriter(String filename, OutputStream out, FabricSinkConfig fabricSinkConfig) {
        this.filename = filename;
        this.fabricSinkConfig = fabricSinkConfig;
        this.plainOutputStream = out;
        try {
            this.writer = OBJECT_MAPPER.getFactory()
                    .createGenerator(out, JsonEncoding.UTF8)
                    .setRootValueSeparator(null);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * @param sinkRecord     the record to persist.
     * @param dataFormat the data format to use.
     * @param headerTransforms the header transforms to apply.
     * @throws IOException if an error occurs while writing the record.
     */
    @Override
    public void write(SinkRecord sinkRecord, IngestionProperties.DataFormat dataFormat,
            HeaderTransforms headerTransforms) throws IOException {
        if (schema == null) {
            schema = sinkRecord.valueSchema();
            LOGGER.debug("Opening record writer for: {}", filename);
        }
        Map<String, Object> parsedHeaders = getHeadersAsMap(sinkRecord, headerTransforms);
        Map<String, String> kafkaMd = getKafkaMetaDataAsMap(sinkRecord);
        if (FormatWriterHelper.INSTANCE.isCsv(dataFormat)) {
            writeCsvRecord(sinkRecord, parsedHeaders, kafkaMd);
        } else {
            Map<String, Object> parsedKeys = convertSinkRecordToMap(sinkRecord, true, dataFormat).stream().reduce(new HashMap<>(),
                    (acc, map) -> {
                        acc.putAll(map);
                        return acc;
                    });
            Collection<Map<String, Object>> parsedValues = convertSinkRecordToMap(sinkRecord, false, dataFormat);
            this.writeJsonRecord(sinkRecord, parsedValues, parsedKeys, parsedHeaders, kafkaMd);
        }
    }

    private void writeJsonRecord(@NotNull SinkRecord sinkRecord, @NotNull Collection<Map<String, Object>> parsedValues,
            Map<String, Object> parsedKeys, Map<String, Object> parsedHeaders,
            Map<String, String> kafkaMd) throws IOException {
        if (parsedValues.isEmpty()) {
            createRecord(sinkRecord, parsedKeys, parsedHeaders, kafkaMd, new HashMap<>());
        } else {
            for (Map<String, Object> parsedValue : parsedValues) {
                Map<String, Object> updatedValue = new HashMap<>(parsedValue);
                createRecord(sinkRecord, parsedKeys, parsedHeaders, kafkaMd, updatedValue);
            }
        }
    }

    private void createRecord(@NotNull SinkRecord sinkRecord, Map<String, Object> parsedKeys,
            Map<String, Object> parsedHeaders, Map<String, String> kafkaMd,
            Map<String, Object> updatedValue) throws IOException {
        boolean payloadAsDynamic = fabricSinkConfig.getTopicToTableMapping(sinkRecord.topic()) != null
                && fabricSinkConfig.getTopicToTableMapping(sinkRecord.topic()).isDynamicPayload();
        Map<String, Object> targetPayload = null;
        if (payloadAsDynamic) {
            targetPayload = new HashMap<>();
            targetPayload.put(PAYLOAD_FIELD, updatedValue);
        } else {
            // Add all the value fields are flat
            targetPayload = updatedValue;
        }
        /* Add all the key fields */
        if (sinkRecord.key() != null) {
            if (parsedKeys.size() == 1 && parsedKeys.containsKey(KEY_FIELD)) {
                targetPayload.put(KEYS_FIELD, parsedKeys.get(KEY_FIELD));
            } else {
                targetPayload.put(KEYS_FIELD, parsedKeys);
            }
        }
        /* End add key fields */
        /* Add record headers */
        if (sinkRecord.headers() != null && !sinkRecord.headers().isEmpty()) {
            targetPayload.put(HEADERS_FIELD, parsedHeaders);
        }
        /* End record headers */
        /* Add metadata fields */
        targetPayload.put(KAFKA_METADATA_FIELD, kafkaMd);
        /* End metadata fields */
        /* Write out each value row with key and header fields */
        writer.writeObject(targetPayload);
        writer.writeRaw(LINE_SEPARATOR);
    }

    private void writeCsvRecord(SinkRecord inSinkRecord, Map<String, Object> parsedHeaders,
            Map<String, String> kafkaMd) throws IOException {
        String serializedKeys = StringEscapeUtils.escapeCsv(convertSinkRecordToCsv(inSinkRecord, true));
        String serializedValues = convertSinkRecordToCsv(inSinkRecord, false);
        String serializedHeaders = StringEscapeUtils.escapeCsv(OBJECT_MAPPER.writeValueAsString(parsedHeaders));
        String serializedMetadata = StringEscapeUtils.escapeCsv(OBJECT_MAPPER.writeValueAsString(kafkaMd));
        String formattedRecord = String.format("%s,%s,%s,%s", serializedValues, serializedKeys,
                serializedHeaders, serializedMetadata);
        LOGGER.trace("Writing record to file: Keys {} , Values {} , Headers {} , OverallRecord {}",
                serializedKeys, serializedValues, serializedHeaders, formattedRecord);
        this.plainOutputStream.write(
                formattedRecord.getBytes(StandardCharsets.UTF_8));
        this.plainOutputStream.write(LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        try {
            writer.close();
            FormatWriterHelper.INSTANCE.close();
        } catch (IOException e) {
            throw new DataException(e);
        }
    }

    @Override
    public void commit() {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new DataException(e);
        }
    }
}
