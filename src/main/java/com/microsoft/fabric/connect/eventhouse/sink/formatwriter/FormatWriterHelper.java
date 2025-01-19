package com.microsoft.fabric.connect.eventhouse.sink.formatwriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.ingest.IngestionProperties;

import io.confluent.kafka.serializers.NonRecordContainer;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat.*;

public enum FormatWriterHelper {
    INSTANCE;

    private static final Logger LOGGER = LoggerFactory.getLogger(FormatWriterHelper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};
    private final JsonConverter keyJsonConverter = new JsonConverter();
    private final JsonConverter valueJsonConverter = new JsonConverter();
    private static FormatWriterHelper instance;

    FormatWriterHelper() {
        keyJsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), true);
        valueJsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public boolean isSchemaFormat(IngestionProperties.DataFormat dataFormat) {
        return dataFormat == JSON || dataFormat == MULTIJSON
                || dataFormat == AVRO || dataFormat == SINGLEJSON;

    }

    boolean isAvro(IngestionProperties.DataFormat dataFormat) {
        return IngestionProperties.DataFormat.AVRO.equals(dataFormat)
                || IngestionProperties.DataFormat.APACHEAVRO.equals(dataFormat);
    }

    public boolean isCsv(IngestionProperties.DataFormat dataFormat) {
        return IngestionProperties.DataFormat.CSV.equals(dataFormat);
    }

    public @NotNull Map<String, Object> convertAvroRecordToMap(Schema schema, Object value) throws IOException {
        Map<String, Object> updatedValue = new HashMap<>();
        if (value != null) {
            if (value instanceof NonRecordContainer) {
                updatedValue.put(schema.name(), ((NonRecordContainer) value).getValue());
            } else {
                if (value instanceof GenericData.Record) {
                    updatedValue.putAll(avroToJson((GenericData.Record) value));
                }
            }
        }
        return updatedValue;
    }

    /**
     * @param messageBytes           Raw message bytes to transform
     * @param defaultKeyOrValueField Default value for Key or Value
     * @param dataformat             JSON or Avro
     * @return a Map of the K-V of JSON
     */
    public @NotNull Collection<Map<String, Object>> convertBytesToMap(byte[] messageBytes,
            String defaultKeyOrValueField,
            IngestionProperties.DataFormat dataformat) throws IOException {
        if (messageBytes == null || messageBytes.length == 0) {
            return Collections.emptyList();
        }
        if (isAvro(dataformat)) {
            return bytesToAvroRecord(defaultKeyOrValueField, messageBytes);
        }
        String bytesAsJson = new String(messageBytes, StandardCharsets.UTF_8);
        if (isJson(dataformat)) {
            return isValidJson(defaultKeyOrValueField, bytesAsJson) ? parseJson(bytesAsJson)
                    : Collections.singletonList(Collections.singletonMap(defaultKeyOrValueField,
                            OBJECT_MAPPER.readTree(messageBytes)));
        } else {
            return Collections.singletonList(Collections.singletonMap(defaultKeyOrValueField,
                    Base64.getEncoder().encodeToString(messageBytes)));
        }
    }

    private static @NotNull Collection<Map<String, Object>> parseJson(String json) throws IOException {
        JsonNode jsonData = OBJECT_MAPPER.readTree(json);
        if (jsonData.isArray()) {
            List<Map<String, Object>> result = new ArrayList<>();
            for (JsonNode node : jsonData) {
                result.add(OBJECT_MAPPER.convertValue(node, MAP_TYPE_REFERENCE));
            }
            return result;
        } else {
            return Collections.singletonList(OBJECT_MAPPER.convertValue(jsonData, MAP_TYPE_REFERENCE));
        }
    }

    /**
     * Convert a given avro record to json and return the encoded bytes.
     * @param genericRecord The GenericRecord to convert
     */
    private Map<String, Object> avroToJson(@NotNull GenericRecord genericRecord) throws IOException {
        return OBJECT_MAPPER.readerFor(MAP_TYPE_REFERENCE).readValue(genericRecord.toString());
    }

    public @NotNull Map<String, Object> structToMap(String topicName,
            @NotNull Struct recordData, boolean isKey) throws IOException {
        JsonConverter jsonConverter = isKey ? keyJsonConverter : valueJsonConverter;
        byte[] jsonBytes = jsonConverter.fromConnectData(topicName, recordData.schema(), recordData);
        return OBJECT_MAPPER.readValue(jsonBytes, MAP_TYPE_REFERENCE);
    }

    private boolean isValidJson(String defaultKeyOrValueField, String json) {
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            if (!parser.nextToken().isStructStart()) {
                LOGGER.debug("No start token found for json {}. Is key {} ", json, defaultKeyOrValueField);
                return false;
            }
            OBJECT_MAPPER.readTree(json);
        } catch (IOException e) {
            LOGGER.debug("Parsed data is not json {} , failed with {}", json, e.getMessage());
            return false;
        }
        return true;
    }

    public @NotNull Map<String, Object> convertStringToMap(Object value,
            String defaultKeyOrValueField,
            IngestionProperties.DataFormat dataFormat) throws IOException {
        String objStr = (String) value;
        if (isJson(dataFormat) && isValidJson(defaultKeyOrValueField, objStr)) {
            return OBJECT_MAPPER.readerFor(MAP_TYPE_REFERENCE).readValue(objStr);
        } else {
            return Collections.singletonMap(defaultKeyOrValueField, objStr);
        }
    }

    public void close() {
        try {
            keyJsonConverter.close();
            valueJsonConverter.close();
        } catch (Exception e) {
            LOGGER.warn("Failed to close JsonConverter", e);
        }
    }

    public boolean isJson(IngestionProperties.DataFormat dataFormat) {
        return IngestionProperties.DataFormat.JSON.equals(dataFormat)
                || IngestionProperties.DataFormat.MULTIJSON.equals(dataFormat)
                || IngestionProperties.DataFormat.SINGLEJSON.equals(dataFormat);
    }

    private @NotNull Collection<Map<String, Object>> bytesToAvroRecord(String defaultKeyOrValueField, byte[] receivedMessage) {
        Map<String, Object> returnValue = new HashMap<>();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(receivedMessage), datumReader)) {
            // avro input parser
            List<Map<String, Object>> nodes = new ArrayList<>();
            while (dataFileReader.hasNext()) {
                String jsonString = dataFileReader.next().toString();
                LOGGER.trace("Encoding AVROBytes yielded {}", jsonString);
                Map<String, Object> nodeMap = OBJECT_MAPPER.readValue(jsonString, MAP_TYPE_REFERENCE);
                returnValue.putAll(nodeMap);
                nodes.add(returnValue);
            }
            return nodes;
        } catch (Exception e) {
            LOGGER.error("Failed to parse AVRO record (3) \n", e);
            return Collections.singletonList(
                    Collections.singletonMap(defaultKeyOrValueField,
                            Base64.getEncoder().encodeToString(receivedMessage)));
        }
    }
}
