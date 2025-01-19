package com.microsoft.fabric.connect.eventhouse.sink.formatwriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.microsoft.fabric.connect.eventhouse.sink.HeaderTransforms;

public abstract class EventHouseRecordWriterBase {
    protected static final String KEYS = "keys";
    protected static final String HEADERS = "headers";
    protected static final String KAFKA_MD = "kafkamd";
    protected static final ObjectMapper RESULT_MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
    protected static final TypeReference<Map<String, Object>> GENERIC_MAP = new TypeReference<Map<String, Object>>() {};

    public HeaderTransforms headerTransforms() throws JsonProcessingException {
        CollectionType resultType = TypeFactory.defaultInstance().constructCollectionType(Set.class, String.class);
        String projectHeaders = "[" + IntStream.range(0, 10)
                .mapToObj(i -> new String[] {String.format("'HeaderInt-%d'", i), String.format("'HeaderBytes-%d'", i)})
                .flatMap(Arrays::stream)
                .collect(Collectors.joining(", ")) + "]";
        String dropHeaders = "[" + IntStream.range(0, 10)
                .mapToObj(i -> String.format("'DropInt-%d'", i))
                .collect(Collectors.joining(", ")) + "]";

        Set<String> headersToProject = RESULT_MAPPER.readValue(projectHeaders, resultType);
        Set<String> headersToDrop = RESULT_MAPPER.readValue(dropHeaders, resultType);
        return new HeaderTransforms(headersToDrop, headersToProject);
    }

    protected void validate(String actualFilePath, Map<Integer, String[]> expectedResultsMap) throws IOException, JSONException {
        // Warns if the types are not generified
        List<String> actualJson = Files.readAllLines(Paths.get(actualFilePath));
        for (int i = 0; i < actualJson.size(); i++) {
            String actual = actualJson.get(i);
            Map<String, Object> actualMap = RESULT_MAPPER.readValue(actual, GENERIC_MAP);
            String[] expected = expectedResultsMap.get(i);
            String actualKeys = RESULT_MAPPER.writeValueAsString(actualMap.get(KEYS));
            String actualHeaders = RESULT_MAPPER.writeValueAsString(actualMap.get(HEADERS));
            JSONAssert.assertEquals(expected[1], actualKeys, false);
            JSONAssert.assertEquals(expected[0], actualHeaders, false);
            // to get the values it is to remove keys and headers , then get all the fields and compare
            actualMap.remove(KEYS);
            actualMap.remove(HEADERS);
            actualMap.remove(KAFKA_MD);
            // Now actualMap contains only the value
            String actualValues = RESULT_MAPPER.writeValueAsString(actualMap);
            if (expected[2] == null) {
                // there are no fields or no keys
                Assertions.assertTrue(actualMap.keySet().isEmpty(), "Expected null value for tombstone record");
            } else {
                JSONAssert.assertEquals(expected[2], actualValues, false);
            }
        }
    }
}
