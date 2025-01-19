package com.microsoft.fabric.connect.eventhouse.sink.formatwriter;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.connect.eventhouse.sink.HeaderTransforms;

import io.confluent.kafka.serializers.NonRecordContainer;

public abstract class HeaderAndMetadataWriter {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    public static final String LINE_SEPARATOR = System.lineSeparator();
    protected static final Logger LOGGER = LoggerFactory.getLogger(HeaderAndMetadataWriter.class);
    public static final String HEADERS_FIELD = "headers";
    public static final String KEYS_FIELD = "keys";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    public static final String KAFKA_METADATA_FIELD = "kafkamd";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String OFFSET = "offset";

    @NotNull
    public Map<String, Object> getHeadersAsMap(@NotNull SinkRecord sinkRecord, @NotNull HeaderTransforms headerTransforms) {
        Map<String, Object> headers = new HashMap<>();
        Set<String> headersToDrop = headerTransforms.getHeadersToDrop();
        Set<String> headersToProject = headerTransforms.getHeadersToProject();
        sinkRecord.headers().forEach(header -> {
            if (shouldDropHeader(header.key(), headersToDrop)) {
                LOGGER.trace("Dropping header: {}", header.key());
            } else if (shouldProjectHeader(header.key(), headersToProject)) {
                headers.put(header.key(), Values.convertToString(header.schema(), header.value()));
            } else if (headersToProject.isEmpty()) {
                headers.put(header.key(), Values.convertToString(header.schema(), header.value()));
            }
        });
        return headers;
    }

    private boolean shouldDropHeader(String headerKey, Set<String> headersToDrop) {
        return headersToDrop != null && !headersToDrop.isEmpty() && headersToDrop.contains(headerKey);
    }

    private boolean shouldProjectHeader(String headerKey, Set<String> headersToProject) {
        return headersToProject != null && !headersToProject.isEmpty() && headersToProject.contains(headerKey);
    }

    /**
     * Convert SinkRecord to CSV
     *
     * @param sinkRecord SinkRecord
     * @param isKey  boolean
     * @return String
     */
    public String convertSinkRecordToCsv(@NotNull SinkRecord sinkRecord, boolean isKey) {
        Object value = isKey ? sinkRecord.key() : sinkRecord.value();
        if (value instanceof byte[]) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        } else {
            return value == null ? "" : value.toString();
        }
    }

    @NotNull
    @SuppressWarnings(value = "unchecked")
    public Collection<Map<String, Object>> convertSinkRecordToMap(@NotNull SinkRecord sinkRecord, boolean isKey,
            IngestionProperties.DataFormat dataFormat) throws IOException {
        Object recordValue;
        Schema schema;
        String defaultKeyOrValueField;
        if (isKey) {
            recordValue = sinkRecord.key();
            schema = sinkRecord.keySchema();
            defaultKeyOrValueField = KEY_FIELD;
        } else {
            recordValue = sinkRecord.value();
            schema = sinkRecord.valueSchema();
            defaultKeyOrValueField = VALUE_FIELD;
        }
        if (recordValue == null) {
            return Collections.emptyList();
        }
        if (recordValue instanceof Struct) {
            Struct recordStruct = (Struct) recordValue;
            return Collections.singletonList(FormatWriterHelper.INSTANCE.structToMap(sinkRecord.topic(), recordStruct, isKey));
        }
        // Is Avro Data
        if (recordValue instanceof GenericData.Record || recordValue instanceof NonRecordContainer) {
            return Collections.singletonList(FormatWriterHelper.INSTANCE.convertAvroRecordToMap(schema, recordValue));
        }
        // String or JSON
        if (recordValue instanceof String) {
            return Collections.singletonList(FormatWriterHelper.INSTANCE.convertStringToMap(recordValue,
                    defaultKeyOrValueField, dataFormat));
        }
        // Map
        if (recordValue instanceof Map) {
            return Collections.singletonList((Map<String, Object>) recordValue);
        }
        // is a byte array
        if (FormatWriterHelper.INSTANCE.isSchemaFormat(dataFormat)) {
            if (recordValue instanceof byte[]) {
                return FormatWriterHelper.INSTANCE.convertBytesToMap((byte[]) recordValue, defaultKeyOrValueField, dataFormat);
            } else {
                String fieldName = isKey ? KEY_FIELD : VALUE_FIELD;
                return Collections.singletonList(Collections.singletonMap(fieldName, recordValue));
            }
        }
        String errorMessage = String.format("DataFormat %s is not supported in the connector though " +
                "it may be supported for ingestion in ADX. Please raise a feature request if a " +
                "new format has to be supported.", dataFormat);
        throw new ConnectException(errorMessage);
    }

    public Map<String, String> getKafkaMetaDataAsMap(@NotNull SinkRecord sinkRecord) {
        Map<String, String> kafkaMetadata = new HashMap<>();
        kafkaMetadata.put(TOPIC, sinkRecord.topic());
        kafkaMetadata.put(PARTITION, String.valueOf(sinkRecord.kafkaPartition()));
        kafkaMetadata.put(OFFSET, String.valueOf(sinkRecord.kafkaOffset()));
        return kafkaMetadata;
    }
}
