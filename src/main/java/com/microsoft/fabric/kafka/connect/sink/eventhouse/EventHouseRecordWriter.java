package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.kafka.connect.sink.format.RecordWriter;
import com.microsoft.fabric.kafka.connect.sink.formatwriter.FormatWriterHelper;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.*;

import static com.microsoft.fabric.kafka.connect.sink.formatwriter.FormatConverter.*;
import static org.apache.commons.lang3.SystemProperties.LINE_SEPARATOR;

public class EventHouseRecordWriter implements RecordWriter {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHouseRecordWriter.class);
    private final List<String> bulkRequests = new ArrayList<>();
    private final EventHouseSinkConfig config;

    private transient volatile Exception flushException;

    public EventHouseRecordWriter(@NotNull EventHouseSinkConfig config) throws URISyntaxException {
        this.config = config;
        LOGGER.info("Initializing the class from KustoSinkWriter");
    }

    public String write(SinkRecord record, IngestionProperties.DataFormat dataFormat) throws IOException {
        Map<String, Object> parsedHeaders = getHeadersAsMap(record);
        Map<String, String> kafkaMd = getKafkaMetaDataAsMap(record);
        if (FormatWriterHelper.isCsv(dataFormat)) {
            String serializedKeys = StringEscapeUtils.escapeCsv(convertSinkRecordToCsv(record, true));
            String serializedValues = convertSinkRecordToCsv(record, false);
            String serializedHeaders = StringEscapeUtils.escapeCsv(OBJECT_MAPPER.writeValueAsString(parsedHeaders));
            String serializedMetadata = StringEscapeUtils.escapeCsv(OBJECT_MAPPER.writeValueAsString(kafkaMd));
            String formattedRecord = String.format("%s,%s,%s,%s", serializedValues, serializedKeys,
                    serializedHeaders, serializedMetadata);
            LOGGER.trace("Writing record to file: Keys {} , Values {} , Headers {} , OverallRecord {}",
                    serializedKeys, serializedValues, serializedHeaders, formattedRecord);
            return formattedRecord;
        } else {
            try (StringWriter jsonObjectWriter = new StringWriter();
                 JsonGenerator writer = OBJECT_MAPPER.getFactory()
                         .createGenerator(jsonObjectWriter)
                         .setRootValueSeparator(null)) {
                Map<String, Object> parsedKeys = convertSinkRecordToMap(record, true, dataFormat).stream().reduce(new HashMap<>(),
                        (acc, map) -> {
                            acc.putAll(map);
                            return acc;
                        });
                Collection<Map<String, Object>> parsedValues = convertSinkRecordToMap(record, false, dataFormat);

                parsedValues.forEach(parsedValue -> {
                    Map<String, Object> updatedValue = (record.value() == null) ? new HashMap<>() : new HashMap<>(parsedValue);
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
                        LOGGER.error("Error serializing record at Kafka co-ordinate: {}", kafkaMd, e);
                        throw new ConnectException(e);
                    }
                });
                return jsonObjectWriter.toString();
            } catch (Exception ex) {
                LOGGER.error("Error serializing record:  {}", kafkaMd, ex);
                throw new ConnectException(ex);
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void commit() {

    }
}
