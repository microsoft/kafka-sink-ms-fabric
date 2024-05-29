package com.microsoft.fabric.kafka.connect.sink.formatwriter;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.kafka.connect.sink.format.RecordWriter;

import static com.microsoft.fabric.kafka.connect.sink.formatwriter.FormatWriterHelper.isCsv;

public class KqlDbRecordWriter implements RecordWriter {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    private static final Logger LOGGER = LoggerFactory.getLogger(KqlDbRecordWriter.class);
    public static final String LINE_SEPARATOR = System.lineSeparator();
    private static final String KAFKA_METADATA_FIELD = "kafkamd";
    private static final String HEADERS_FIELD = "headers";
    private static final String KEYS_FIELD = "keys";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";


    private final String filename;
    private final JsonGenerator writer;
    private final OutputStream plainOutputStream;
    private Schema schema;
    private final HeaderAndMetadataWriter headerAndMetadataWriter;

    public KqlDbRecordWriter(String filename, OutputStream out) {
         this.headerAndMetadataWriter = new HeaderAndMetadataWriter();
        this.filename = filename;
        this.plainOutputStream = out;
        try {
            this.writer = OBJECT_MAPPER.getFactory()
                    .createGenerator(out)
                    .setRootValueSeparator(null);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * @param record     the record to persist.
     * @param dataFormat the data format to use.
     * @throws IOException if an error occurs while writing the record.
     */
    @Override
    public void write(SinkRecord record, IngestionProperties.DataFormat dataFormat) throws IOException {
        if (schema == null) {
            schema = record.valueSchema();
            LOGGER.debug("Opening record writer for: {}", filename);
        }
        Map<String, Object> parsedHeaders = headerAndMetadataWriter.getHeadersAsMap(record);
        Map<String, String> kafkaMd = headerAndMetadataWriter.getKafkaMetaDataAsMap(record);
        if (isCsv(dataFormat)) {
            String serializedKeys = StringEscapeUtils.escapeCsv(headerAndMetadataWriter.convertSinkRecordToCsv(record, true));
            String serializedValues = headerAndMetadataWriter.convertSinkRecordToCsv(record, false);
            String serializedHeaders = StringEscapeUtils.escapeCsv(OBJECT_MAPPER.writeValueAsString(parsedHeaders));
            String serializedMetadata = StringEscapeUtils.escapeCsv(OBJECT_MAPPER.writeValueAsString(kafkaMd));
            String formattedRecord = String.format("%s,%s,%s,%s", serializedValues, serializedKeys,
                    serializedHeaders, serializedMetadata);
            LOGGER.trace("Writing record to file: Keys {} , Values {} , Headers {} , OverallRecord {}",
                    serializedKeys, serializedValues, serializedHeaders, formattedRecord);
            this.plainOutputStream.write(
                    formattedRecord.getBytes(StandardCharsets.UTF_8));
            this.plainOutputStream.write(LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8));
        } else {
            Map<String, Object> parsedKeys = headerAndMetadataWriter.convertSinkRecordToMap(record, true, dataFormat).stream().reduce(new HashMap<>(),
                    (acc, map) -> {
                        acc.putAll(map);
                        return acc;
                    });
            Collection<Map<String, Object>> parsedValues = headerAndMetadataWriter.convertSinkRecordToMap(record, false, dataFormat);

            parsedValues.forEach(parsedValue -> {
                Map<String, Object> updatedValue = (record.value() == null) ? new HashMap<>() :
                        new HashMap<>(parsedValue);
                /* Add all the key fields */
                if (record.key() != null) {
                    if (parsedKeys.size() == 1 && parsedKeys.containsKey(KEY_FIELD)) {
                        updatedValue.put(KEYS_FIELD, parsedKeys.get(KEY_FIELD));
                    } else {
                        updatedValue.put(KEYS_FIELD, parsedKeys);
                    }
                }
                /* End add key fields */
                /* Add record headers */
                if (record.headers() != null && !record.headers().isEmpty()) {
                    updatedValue.put(HEADERS_FIELD, parsedHeaders);
                }
                /* End record headers */
                /* Add metadata fields */
                updatedValue.put(KAFKA_METADATA_FIELD, kafkaMd);
                /* End metadata fields */
                try {
                    /* Write out each value row with key and header fields */
                    writer.writeObject(updatedValue);
                    writer.writeRaw(LINE_SEPARATOR);
                } catch (IOException e) {
                    LOGGER.error("Error writing record to file: {}", filename, e);
                    throw new ConnectException(e);
                }
            });
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
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
