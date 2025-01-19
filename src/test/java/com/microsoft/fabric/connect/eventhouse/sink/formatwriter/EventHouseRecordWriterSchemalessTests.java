package com.microsoft.fabric.connect.eventhouse.sink.formatwriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.fabric.connect.eventhouse.sink.Utils;
import com.microsoft.fabric.connect.eventhouse.sink.format.RecordWriter;

import io.confluent.avro.random.generator.Generator;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

class EventHouseRecordWriterSchemalessTests extends EventHouseRecordWriterBase {
    @ParameterizedTest(name = "JSON data serialized as bytes with key schema {0} and " +
            "value schema {1} should be deserialized correctly. Simple key: {2}, Simple value: {3}")
    @CsvSource({
            "avro-simple-schema.json,avro-struct-schema.json,true,false",
            "avro-struct-schema.json,avro-struct-schema.json,false,false",
            "avro-simple-schema.json,avro-simple-schema.json,true,true"
    })
    void validateJsonSerializedAsBytes(String keySchemaPath, String valueSchemaPath,
            boolean isSimpleKey, boolean isSimpleValue)
            throws IOException, JSONException {
        List<SinkRecord> records = new ArrayList<>();
        Generator randomAvroValueData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(String.format("avro-schemas/%s", valueSchemaPath)))).build();
        Generator randomAvroKeyData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(String.format("avro-schemas/%s", keySchemaPath)))).build();
        JsonAvroConverter converter = new JsonAvroConverter();
        Map<Integer, String[]> expectedResultsMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Object avroKey = randomAvroKeyData.generate();
            Object key = avroKey.toString().getBytes(StandardCharsets.UTF_8);
            Object avroValue = randomAvroValueData.generate();
            Object value = avroValue.toString().getBytes(StandardCharsets.UTF_8);
            SinkRecord sinkRecord = new SinkRecord("bytes.record.topic", i % 3,
                    null,
                    key,
                    null,
                    value,
                    i);
            sinkRecord.headers().addInt(String.format("HeaderInt-%s", i), i);
            records.add(sinkRecord);
            String expectedValueString = isSimpleValue ? RESULT_MAPPER.writeValueAsString(Collections.singletonMap("value", avroValue))
                    : new String(converter.convertToJson((GenericRecord) avroValue), StandardCharsets.UTF_8);
            String expectedKeyString = isSimpleKey ? avroKey.toString() : new String(converter.convertToJson((GenericRecord) avroKey), StandardCharsets.UTF_8);
            String expectedHeaderJson = RESULT_MAPPER.writeValueAsString(Collections.singletonMap(
                    String.format("HeaderInt-%s", i), String.valueOf(i)));
            expectedResultsMap.put(i, new String[] {expectedHeaderJson, expectedKeyString, expectedValueString});
        }
        File file = new File(String.format("%s.%s", UUID.randomUUID(), "json"));
        Utils.restrictPermissions(file);
        EventHouseRecordWriterProvider writer = new EventHouseRecordWriterProvider();
        OutputStream out = Files.newOutputStream(file.toPath());
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
        for (SinkRecord sinkRecord : records) {
            rd.write(sinkRecord, IngestionProperties.DataFormat.JSON, headerTransforms());
        }
        rd.commit();
        validate(file.getPath(), expectedResultsMap);
        rd.close();
        FileUtils.deleteQuietly(file);
    }

    @ParameterizedTest(name = "AVRO Data to be serialized with key schema {0} and value schema {1} isSimpleKey {2} isSimpleValue {3}")
    @CsvSource({
            "avro-simple-schema.json,avro-struct-schema.json,true,false",
            "avro-struct-schema.json,avro-struct-schema.json,false,false",
            "avro-simple-schema.json,avro-simple-schema.json,true,true"
    })
    void validateAvroDataSerializedAsBytes(String keySchemaPath, String valueSchemaPath, boolean isSimpleKey, boolean isSimpleValue)
            throws IOException, JSONException, NoSuchAlgorithmException {
        List<SinkRecord> records = new ArrayList<>();
        Generator randomAvroValueData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(String.format("avro-schemas/%s", valueSchemaPath)))).build();
        Generator randomAvroKeyData = new Generator.Builder().schemaStream(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(String.format("avro-schemas/%s", keySchemaPath)))).build();
        JsonAvroConverter converter = new JsonAvroConverter();
        Map<Integer, String[]> expectedResultsMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Object key = randomAvroKeyData.generate();
            Object value = randomAvroValueData.generate();
            SinkRecord sinkRecord = new SinkRecord("avro.bytes.record.topic", i % 3,
                    null,
                    key,
                    null,
                    value,
                    i);
            sinkRecord.headers().addInt(String.format("HeaderInt-%s", i), i);
            byte[] bytesData = new byte[20];
            SecureRandom.getInstanceStrong().nextBytes(bytesData);
            sinkRecord.headers().addBytes(String.format("HeaderBytes-%s", i), bytesData);
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
        RecordWriter rd = writer.getRecordWriter(file.getPath(), out);
        for (SinkRecord sinkRecord : records) {
            rd.write(sinkRecord, IngestionProperties.DataFormat.AVRO, headerTransforms());
        }
        rd.commit();
        validate(file.getPath(), expectedResultsMap);
        rd.close();
        FileUtils.deleteQuietly(file);
    }
}
