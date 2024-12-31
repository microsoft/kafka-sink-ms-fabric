package com.microsoft.fabric.connect.eventhouse.sink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FabricSinkConnectorTest {

    @Test
    public void testStart() {
        FabricSinkConnector fabricSinkConnector = new FabricSinkConnector();
        Map<String, String> mockProps = new HashMap<>();
        mockProps.put("kusto.ingestion.url", "testValue");
        mockProps.put("kusto.query.url", "testValue");
        mockProps.put("aad.auth.appkey", "testValue");
        mockProps.put("aad.auth.appid", "testValue");
        mockProps.put("aad.auth.authority", "testValue");
        mockProps.put("kusto.tables.topics.mapping", "testValue");
        fabricSinkConnector.start(mockProps);
        Assertions.assertNotNull(fabricSinkConnector);
        Assertions.assertNotNull(fabricSinkConnector.config());
        Assertions.assertEquals(fabricSinkConnector.config().configKeys().get("kusto.ingestion.url").name, "kusto.ingestion.url");
        Assertions.assertEquals(fabricSinkConnector.config().configKeys().get("kusto.query.url").name, "kusto.query.url");
    }

    @Test
    public void testTaskConfigs() {
        FabricSinkConnector fabricSinkConnector = new FabricSinkConnector();

        Map<String, String> mockProps = new HashMap<>();
        mockProps.put("kusto.ingestion.url", "testValue");
        mockProps.put("kusto.query.url", "testValue");
        mockProps.put("aad.auth.appkey", "testValue");
        mockProps.put("aad.auth.appid", "testValue");
        mockProps.put("aad.auth.authority", "testValue");
        mockProps.put("kusto.tables.topics.mapping", "testValue");

        fabricSinkConnector.start(mockProps);

        List<Map<String, String>> mapList = fabricSinkConnector.taskConfigs(0);
        Assertions.assertNotNull(fabricSinkConnector);
        Assertions.assertNotNull(fabricSinkConnector.config());
        Assertions.assertEquals(fabricSinkConnector.config().configKeys().get("kusto.ingestion.url").name, "kusto.ingestion.url");
        Assertions.assertEquals(fabricSinkConnector.config().configKeys().get("kusto.query.url").name, "kusto.query.url");

    }

}
