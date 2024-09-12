package com.microsoft.fabric.kafka.connect.sink.formatwriter;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.microsoft.fabric.kafka.connect.sink.formatwriter.FormatWriterHelper.isSchemaFormat;

public class FormatConverter {
    public static final String KEY_FIELD = "key";
    public static final String KEYS_FIELD = "keys";
    public static final String HEADERS_FIELD = "headers";
    public static final String VALUE_FIELD = "value";

    public static final String KAFKA_METADATA_FIELD = "kafkamd";
    public static String TOPIC = "topic";
    public static String PARTITION = "partition";
    public static String OFFSET = "offset";

    @NotNull
    public static Map<String, Object> getHeadersAsMap(@NotNull SinkRecord record) {
        Map<String, Object> headers = new HashMap<>();
        record.headers().forEach(header -> headers.put(header.key(), header.value()));
        return headers;
    }

    /**
     * Convert SinkRecord to CSV
     *
     * @param record SinkRecord
     * @param isKey  boolean
     * @return String
     */
    public static String convertSinkRecordToCsv(@NotNull SinkRecord record, boolean isKey) {
        if (isKey) {
            if (record.key() instanceof byte[]) {
                return record.key() == null ? "" : new String((byte[]) record.key(), StandardCharsets.UTF_8);
            } else {
                return record.key() == null ? "" : record.key().toString();
            }
        } else {
            if (record.value() instanceof byte[]) {
                return record.value() == null ? "" : new String((byte[]) record.value(), StandardCharsets.UTF_8);
            } else {
                return record.value() == null ? "" : record.value().toString();
            }
        }
    }

    @NotNull
    @SuppressWarnings(value = "unchecked")
    public static Collection<Map<String, Object>> convertSinkRecordToMap(@NotNull SinkRecord record, boolean isKey,
                                                                         IngestionProperties.DataFormat dataFormat) throws IOException {
        Object recordValue = isKey ? record.key() : record.value();
        Schema schema = isKey ? record.keySchema() : record.valueSchema();
        String defaultKeyOrValueField = isKey ? KEY_FIELD : VALUE_FIELD;
        if (recordValue == null) {
            return Collections.emptyList();
        }
        if (recordValue instanceof Struct) {
            Struct recordStruct = (Struct) recordValue;
            return Collections.singletonList(FormatWriterHelper.structToMap(recordStruct));
        }
        // Is Avro Data
        if (recordValue instanceof GenericData.Record || recordValue instanceof NonRecordContainer) {
            return Collections.singletonList(FormatWriterHelper.convertAvroRecordToMap(schema, recordValue));
        }
        // String or JSON
        if (recordValue instanceof String) {
            return Collections.singletonList(FormatWriterHelper.convertStringToMap(recordValue,
                    defaultKeyOrValueField, dataFormat));
        }
        // Map
        if (recordValue instanceof Map) {
            return Collections.singletonList((Map<String, Object>) recordValue);
        }
        // is a byte array
        if (isSchemaFormat(dataFormat)) {
            if (recordValue instanceof byte[]) {
                return FormatWriterHelper.convertBytesToMap((byte[]) recordValue, defaultKeyOrValueField, dataFormat);
            } else {
                String fieldName = isKey ? KEY_FIELD : VALUE_FIELD;
                return Collections.singletonList(Collections.singletonMap(fieldName, recordValue));
            }
        } else {
            String errorMessage = String.format("DataFormat %s is not supported in the connector though " +
                    "it may be supported for ingestion in ADX. Please raise a feature request if a " +
                    "new format has to be supported.", dataFormat);
            throw new ConnectException(errorMessage);
        }
    }

    public static @NotNull Map<String, String> getKafkaMetaDataAsMap(@NotNull SinkRecord record) {
        Map<String, String> kafkaMetadata = new HashMap<>();
        kafkaMetadata.put(TOPIC, record.topic());
        kafkaMetadata.put(PARTITION, String.valueOf(record.kafkaPartition()));
        kafkaMetadata.put(OFFSET, String.valueOf(record.kafkaOffset()));
        return kafkaMetadata;
    }
}