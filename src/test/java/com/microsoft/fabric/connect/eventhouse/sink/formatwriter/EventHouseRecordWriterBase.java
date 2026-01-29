package com.microsoft.fabric.connect.eventhouse.sink.formatwriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;
import org.skyscreamer.jsonassert.comparator.DefaultComparator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConfig;
import com.microsoft.fabric.connect.eventhouse.sink.HeaderTransforms;

import static com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConnectorConfigTest.setupConfigs;

public abstract class EventHouseRecordWriterBase {
    protected static final String KEYS = "keys";
    protected static final String HEADERS = "headers";
    protected static final String KAFKA_MD = "kafkamd";
    protected static final ObjectMapper RESULT_MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
    protected static final TypeReference<Map<String, Object>> GENERIC_MAP = new TypeReference<>() {
    };
    protected static final FabricSinkConfig FABRIC_SINK_CONFIG = new FabricSinkConfig(setupConfigs());

    // Custom comparator that handles floating-point comparison with tolerance
    private static class FloatToleranceComparator extends DefaultComparator {
        private static final double TOLERANCE = 0.01;

        public FloatToleranceComparator(JSONCompareMode mode) {
            super(mode);
        }

        @Override
        public void compareValues(String prefix, Object expectedValue, Object actualValue, JSONCompareResult result) throws JSONException {
            if (expectedValue instanceof Number && actualValue instanceof Number) {
                double expected = ((Number) expectedValue).doubleValue();
                double actual = ((Number) actualValue).doubleValue();
                if (Math.abs(expected - actual) < TOLERANCE) {
                    // Values are within tolerance, consider them equal
                    return;
                }
            }
            // For non-numeric values or values outside tolerance, use default comparison
            super.compareValues(prefix, expectedValue, actualValue, result);
        }
    }

    public HeaderTransforms headerTransforms() throws JsonProcessingException {
        CollectionType resultType = TypeFactory.defaultInstance().constructCollectionType(Set.class, String.class);
        String projectHeaders = "[" + IntStream.range(0, 10)
                .mapToObj(i -> new String[] {"'HeaderInt-%d'".formatted(i), "'HeaderBytes-%d'".formatted(i)})
                .flatMap(Arrays::stream)
                .collect(Collectors.joining(", ")) + "]";
        String dropHeaders = "[" + IntStream.range(0, 10)
                .mapToObj("'DropInt-%d'"::formatted)
                .collect(Collectors.joining(", ")) + "]";

        Set<String> headersToProject = RESULT_MAPPER.readValue(projectHeaders, resultType);
        Set<String> headersToDrop = RESULT_MAPPER.readValue(dropHeaders, resultType);
        return new HeaderTransforms(headersToDrop, headersToProject);
    }

    protected void validate(String actualFilePath, Map<Integer, String[]> expectedResultsMap) throws IOException, JSONException {
        // Warns if the types are not generified
        List<String> actualJson = Files.readAllLines(Path.of(actualFilePath));
        // Create a custom comparator that applies float tolerance
        FloatToleranceComparator comparator = new FloatToleranceComparator(JSONCompareMode.LENIENT);

        for (int i = 0; i < actualJson.size(); i++) {
            String actual = actualJson.get(i);
            Map<String, Object> actualMap = RESULT_MAPPER.readValue(actual, GENERIC_MAP);
            String[] expected = expectedResultsMap.get(i);
            String actualKeys = RESULT_MAPPER.writeValueAsString(actualMap.get(KEYS));
            String actualHeaders = RESULT_MAPPER.writeValueAsString(actualMap.get(HEADERS));
            JSONAssert.assertEquals(expected[1], actualKeys, comparator);
            JSONAssert.assertEquals(expected[0], actualHeaders, comparator);
            // to get the values it is to remove keys and headers , then get all the fields and compare
            actualMap.remove(KEYS);
            actualMap.remove(HEADERS);
            actualMap.remove(KAFKA_MD);
            // Now actualMap contains only the value
            String actualValues = RESULT_MAPPER.writeValueAsString(actualMap);
            if (expected[2] == null) {
                // there are no fields or no keys
                Assertions.assertTrue(actualMap.isEmpty(), "Expected null value for tombstone record");
            } else {
                JSONAssert.assertEquals(expected[2], actualValues, comparator);
            }
        }
    }
}
