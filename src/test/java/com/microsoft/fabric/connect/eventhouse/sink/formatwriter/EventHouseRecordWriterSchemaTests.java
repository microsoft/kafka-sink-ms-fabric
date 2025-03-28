package com.microsoft.fabric.connect.eventhouse.sink.formatwriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.connect.eventhouse.sink.Utils;
import com.microsoft.fabric.connect.eventhouse.sink.format.RecordWriter;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

class EventHouseRecordWriterSchemaTests extends EventHouseRecordWriterBase {
    private static @NotNull Stream<Arguments> testMapSchemaJson() {
        // Key schema, value schema, expectedKey, expectedValue
        Schema intToIntSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).name("IntToIntMap").build();
        Schema stringToIntSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).name("StringToIntMap").build();
        Schema stringToOptionalIntSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA).name("StringToOptionalInt").build();
        Schema arrayOfInts = SchemaBuilder.array(Schema.INT32_SCHEMA).name("ArrayOfInts").build();
        Schema simpleLongSchema = SchemaBuilder.struct().field("recordKey", Schema.INT64_SCHEMA).name("SimpleLongSchema").build();
        Schema structSchema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA).name("StructSchema").build();

        Map<Integer, Integer> intToIntMap = Collections.singletonMap(0, 12);
        Map<String, Integer> stringToIntMap = Collections.singletonMap("String-42", 32);
        Map<String, Integer> stringToOptionalIntMap = Collections.singletonMap("NullString-42", null);
        Map<String, Integer> stringToOptionalIntMapMultiple = new HashMap<>();
        stringToOptionalIntMapMultiple.put("NullString-42", null);
        stringToOptionalIntMapMultiple.put("String-42", 32);

        return Stream.of(
                Arguments.of(intToIntSchema, stringToIntSchema, intToIntMap, stringToIntMap, false, false),
                Arguments.of(stringToIntSchema, stringToOptionalIntSchema, stringToIntMap, stringToOptionalIntMap, false, false),
                Arguments.of(stringToIntSchema, stringToOptionalIntSchema, stringToIntMap, stringToOptionalIntMapMultiple, false, false),
                Arguments.of(stringToIntSchema, arrayOfInts, stringToIntMap, new Integer[] {1, 2, 3, 5, 8, 13, 21}, false, true),
                Arguments.of(simpleLongSchema, structSchema, Collections.singletonMap("recordKey", 42L),
                        "{\"field1\":true,\"field2\":\"Field-@42\"}", false, false),
                Arguments.of(simpleLongSchema, structSchema, Collections.singletonMap("recordKey", 42L), null, false, false));
    }

    @ParameterizedTest(name = "AVRO Data to be serialized with key schema {0} and value schema {1} isSimpleKey {2} isSimpleValue {3}")
    @CsvSource({
            "avro-simple-schema.json,avro-struct-schema.json,true,false",
            "avro-struct-schema.json,avro-struct-schema.json,false,false",
            "avro-simple-schema.json,avro-simple-schema.json,true,true"
    })
    void validateAvroDataToBeSerialized(String keySchemaPath, String valueSchemaPath, boolean isSimpleKey, boolean isSimpleValue)
            throws IOException, JSONException, NoSuchAlgorithmException {
        List<SinkRecord> records = new ArrayList<>();
        Generator randomAvroValueData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(String.format("avro-schemas/%s", valueSchemaPath)))).build();
        Generator randomAvroKeyData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(String.format("avro-schemas/%s", keySchemaPath)))).build();
        AvroData avroDataCache = new AvroData(50);
        Schema keySchema = avroDataCache.toConnectSchema(randomAvroKeyData.schema());
        Schema valueSchema = avroDataCache.toConnectSchema(randomAvroValueData.schema());
        JsonAvroConverter converter = new JsonAvroConverter();
        Map<Integer, String[]> expectedResultsMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Object key = randomAvroKeyData.generate();
            Object value = randomAvroValueData.generate();
            SinkRecord sinkRecord = new SinkRecord("avro.record.topic", i % 3,
                    keySchema,
                    key,
                    valueSchema,
                    value,
                    i);
            sinkRecord.headers().addInt(String.format("HeaderInt-%s", i), i);
            sinkRecord.headers().addInt(String.format("DropInt-%s", i), i);
            byte[] bytesData = new byte[20];
            SecureRandom.getInstanceStrong().nextBytes(bytesData);
            sinkRecord.headers().addBytes(String.format("HeaderBytes-%s", i), bytesData);
            sinkRecord.headers().addBytes(String.format("DropBytes-%s", i), bytesData);
            records.add(sinkRecord);
            String expectedValueString = isSimpleValue ? RESULT_MAPPER.writeValueAsString(Collections.singletonMap("value", value))
                    : new String(converter.convertToJson((GenericData.Record) value));
            String expectedKeyString = isSimpleKey ? key.toString() : new String(converter.convertToJson((GenericData.Record) key));
            Map<String, Object> headerMap = new HashMap<>();
            headerMap.put(String.format("HeaderInt-%s", i), String.valueOf(i));
            headerMap.put(String.format("HeaderBytes-%s", i), bytesData);
            String expectedHeaderJson = RESULT_MAPPER.writeValueAsString(headerMap);
            expectedResultsMap.put(i, new String[] {expectedHeaderJson, expectedKeyString, expectedValueString});
        }
        File file = new File(String.format("%s.%s", UUID.randomUUID(), "json"));
        Utils.restrictPermissions(file);
        EventHouseRecordWriterProvider writer = new EventHouseRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out, FABRIC_SINK_CONFIG);
        for (SinkRecord sinkRecord : records) {
            rd.write(sinkRecord, IngestionProperties.DataFormat.AVRO, headerTransforms());
        }
        rd.commit();
        validate(file.getPath(), expectedResultsMap);
        rd.close();
        FileUtils.deleteQuietly(file);
    }

    // Idea is to use Avro Schema to generate Avro data and convert them to random JSON for tests
    @ParameterizedTest(name = "JSON Data to be serialized with key schema {0} and value schema {1} isSimpleKey {2} isSimpleValue {3}")
    @CsvSource({
            "avro-simple-schema.json,avro-struct-schema.json,true,false",
            "avro-struct-schema.json,avro-struct-schema.json,false,false",
            "avro-simple-schema.json,avro-simple-schema.json,true,true"
    })
    void validateJsonDataToBeSerialized(String keySchemaPath, String valueSchemaPath, boolean isSimpleKey, boolean isSimpleValue)
            throws IOException, JSONException {
        List<SinkRecord> records = new ArrayList<>();
        Generator randomAvroValueData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(String.format("avro-schemas/%s", valueSchemaPath)))).build();
        Generator randomAvroKeyData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(String.format("avro-schemas/%s", keySchemaPath)))).build();

        Map<Integer, String[]> expectedResultsMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Object key = randomAvroKeyData.generate().toString();
            Object value = randomAvroValueData.generate().toString();
            SinkRecord sinkRecord = new SinkRecord("json.record.topic", i % 3,
                    null,
                    key,
                    null,
                    value,
                    i);
            sinkRecord.headers().addInt(String.format("HeaderInt-%s", i), i);
            records.add(sinkRecord);

            String expectedValueString = isSimpleValue ? RESULT_MAPPER.writeValueAsString(Collections.singletonMap("value", value)) : value.toString();
            String expectedKeyString = isSimpleKey ? RESULT_MAPPER.writeValueAsString(key) : key.toString();
            String expectedHeaderJson = RESULT_MAPPER.writeValueAsString(Collections.singletonMap(String.format("HeaderInt-%s", i), String.valueOf(i)));
            expectedResultsMap.put(i, new String[] {expectedHeaderJson, expectedKeyString, expectedValueString});
        }
        File file = new File(String.format("%s.%s", UUID.randomUUID(), "json"));
        Utils.restrictPermissions(file);
        EventHouseRecordWriterProvider writer = new EventHouseRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out, FABRIC_SINK_CONFIG);
        for (SinkRecord sinkRecord : records) {
            rd.write(sinkRecord, IngestionProperties.DataFormat.JSON, headerTransforms());
        }
        rd.commit();
        validate(file.getPath(), expectedResultsMap);
        rd.close();
        FileUtils.deleteQuietly(file);
    }

    @ParameterizedTest(name = "Map Data to be serialized with key schema {0}.name() and value schema {1}.name()")
    @MethodSource("testMapSchemaJson")
    void collectionsSerializationTests(Schema keySchema, Schema valueSchema,
            Map<?, ?> keyValues, Object expectedValues,
            boolean isSimpleKey, boolean isSimpleValue) throws IOException, JSONException {
        // Set up
        Map<Integer, String[]> expectedResultsMap = new HashMap<>();
        SinkRecord sinkRecord = new SinkRecord("json.map.record.topic", 0,
                keySchema,
                keyValues,
                valueSchema,
                expectedValues,
                0);
        sinkRecord.headers().addInt(String.format("HeaderInt-%s", 0), 0);
        String expectedKeyString = isSimpleKey ? RESULT_MAPPER.writeValueAsString(Collections.singletonMap("key", keyValues))
                : RESULT_MAPPER.writeValueAsString(keyValues);
        // Sometimes the input is a JSON string. No need to double encode. Check the struct test
        String expectedValueString;
        if (expectedValues == null) {
            expectedValueString = null;
        } else if (expectedValues instanceof String) {
            expectedValueString = expectedValues.toString();
        } else if (isSimpleValue) {
            expectedValueString = RESULT_MAPPER.writeValueAsString(Collections.singletonMap("value", expectedValues));
        } else {
            expectedValueString = RESULT_MAPPER.writeValueAsString(expectedValues);
        }
        String expectedHeaderJson = RESULT_MAPPER.writeValueAsString(
                Collections.singletonMap(String.format("HeaderInt-%s", 0), "0"));
        expectedResultsMap.put(0, new String[] {expectedHeaderJson, expectedKeyString, expectedValueString});

        // Act
        File file = new File(String.format("%s.%s", UUID.randomUUID(), "json"));
        Utils.restrictPermissions(file);
        EventHouseRecordWriterProvider writer = new EventHouseRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out, FABRIC_SINK_CONFIG);
        rd.write(sinkRecord, IngestionProperties.DataFormat.JSON, headerTransforms());
        // verify
        validate(file.getPath(), expectedResultsMap);
        rd.commit();
        rd.close();
        FileUtils.deleteQuietly(file);
    }
}
