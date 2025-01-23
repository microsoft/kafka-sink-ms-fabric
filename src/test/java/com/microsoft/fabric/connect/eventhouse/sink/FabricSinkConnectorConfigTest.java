package com.microsoft.fabric.connect.eventhouse.sink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import static com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConfig.KUSTO_SINK_ENABLE_TABLE_VALIDATION;
import static org.junit.jupiter.api.Assertions.fail;

public class FabricSinkConnectorConfigTest {
    private static final String DM_URL = "https://ingest-cluster_name.kusto.windows.net";
    private static final String ENGINE_URL = "https://cluster_name.kusto.windows.net";

    @Test
    void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        FabricSinkConfig config = new FabricSinkConfig(setupConfigs());
        Assertions.assertNotNull(config);
    }

    @Test
    void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        FabricSinkConfig config = new FabricSinkConfig(setupConfigs());
        Assertions.assertNotNull(config.getKustoIngestUrl());
        Assertions.assertTrue(config.getFlushSizeBytes() > 0);
        Assertions.assertTrue(config.getFlushInterval() > 0);
        Assertions.assertFalse(config.isDlqEnabled());
        Assertions.assertEquals(FabricSinkConfig.BehaviorOnError.FAIL, config.getBehaviorOnError());
    }

    @Test
    void shouldThrowExceptionWhenKustoURLNotGiven() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(FabricSinkConfig.KUSTO_INGEST_URL_CONF);
        FabricSinkConfig config = new FabricSinkConfig(settings);
        Assertions.assertThrows(ConfigException.class, config::validateConfig);
    }

    @ParameterizedTest
    @CsvSource({
            "Data Source=https://somecluster.southeastasia.dev.kusto.windows.net;" +
                    ";Authority Id=T-1234" +
                    ",https://ingest-somecluster.southeastasia.dev.kusto.windows.net" +
                    ",https://somecluster.southeastasia.dev.kusto.windows.net",
            "Data Source=https://mycluster.southeastasia.dev.kusto.windows.net;" +
                    ";AppClientId=APP-ID;AppKey=Secret-Key;Authority Id=T-1234" +
                    ",https://ingest-mycluster.southeastasia.dev.kusto.windows.net" +
                    ",https://mycluster.southeastasia.dev.kusto.windows.net"

    })
    void shouldReturnEventHouseUrlsFromConnectionString(String connectionString, String ingestUrl, String engineUrl) {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(FabricSinkConfig.KUSTO_INGEST_URL_CONF);
        settings.remove(FabricSinkConfig.KUSTO_ENGINE_URL_CONF);
        settings.put(FabricSinkConfig.CONNECTION_STRING, connectionString);
        FabricSinkConfig config = new FabricSinkConfig(settings);
        Assertions.assertEquals(ingestUrl, config.getKustoIngestUrl());
        Assertions.assertEquals(engineUrl, config.getKustoEngineUrl());
    }

    @Test
    void shouldUseKustoEngineUrlWhenGiven() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(FabricSinkConfig.KUSTO_ENGINE_URL_CONF, ENGINE_URL);
        FabricSinkConfig config = new FabricSinkConfig(settings);
        String kustoEngineUrl = config.getKustoEngineUrl();
        Assertions.assertEquals(ENGINE_URL, kustoEngineUrl);
    }

    @Test
    void shouldThrowExceptionWhenAppIdNotGivenForApplicationAuth() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.put(FabricSinkConfig.KUSTO_AUTH_STRATEGY_CONF,
                FabricSinkConfig.KustoAuthenticationStrategy.APPLICATION.name());
        settings.remove(FabricSinkConfig.KUSTO_AUTH_APPID_CONF);
        FabricSinkConfig config = new FabricSinkConfig(settings);
        // In the previous PR this behavior was changed. The default was to use APPLICATION auth, but it also permits
        // MI. In case of MI the App-ID/App-KEY became optional
        Assertions.assertThrows(ConfigException.class, () -> EventHouseSinkTask.createKustoEngineConnectionString(config, config.getKustoEngineUrl()));
    }

    @Test
    void shouldNotThrowExceptionWhenAppIdNotGivenForManagedIdentity() {
        // Same test as above. In this case since the Auth method is MI AppId/Key becomes optional.
        HashMap<String, String> settings = setupConfigs();
        settings.put(FabricSinkConfig.KUSTO_AUTH_STRATEGY_CONF,
                FabricSinkConfig.KustoAuthenticationStrategy.MANAGED_IDENTITY.name());
        settings.remove(FabricSinkConfig.KUSTO_AUTH_APPID_CONF);
        FabricSinkConfig config = new FabricSinkConfig(settings);
        ConnectionStringBuilder kcsb = EventHouseSinkTask.createKustoEngineConnectionString(config, config.getKustoEngineUrl());
        Assertions.assertNotNull(kcsb);
    }

    @Test
    void shouldFailWhenBehaviorOnErrorIsIllConfigured() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(FabricSinkConfig.KUSTO_INGEST_URL_CONF);
        settings.put(FabricSinkConfig.KUSTO_BEHAVIOR_ON_ERROR_CONF, "DummyValue");
        Assertions.assertThrows(ConfigException.class, () -> new FabricSinkConfig(settings));
    }

    @Test
    void verifyDlqSettings() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(FabricSinkConfig.KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF, "localhost:8081,localhost:8082");
        settings.put(FabricSinkConfig.KUSTO_DLQ_TOPIC_NAME_CONF, "dlq-error-topic");
        FabricSinkConfig config = new FabricSinkConfig(settings);
        Assertions.assertTrue(config.isDlqEnabled());
        Assertions.assertEquals(Arrays.asList("localhost:8081", "localhost:8082"), config.getDlqBootstrapServers());
        Assertions.assertEquals("dlq-error-topic", config.getDlqTopicName());
    }

    @Test
    void shouldProcessDlqConfigsWithPrefix() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.put("misc.deadletterqueue.security.protocol", "SASL_PLAINTEXT");
        settings.put("misc.deadletterqueue.sasl.mechanism", "PLAIN");
        try {
            FabricSinkConfig config = new FabricSinkConfig(settings);
            Assertions.assertNotNull(config);
            Properties dlqProps = config.getDlqProps();
            Assertions.assertEquals("SASL_PLAINTEXT", dlqProps.get("security.protocol"));
            Assertions.assertEquals("PLAIN", dlqProps.get("sasl.mechanism"));
        } catch (Exception ex) {
            fail(ex);
        }
    }

    @Test
    void shouldNotPerformTableValidationByDefault() {
        FabricSinkConfig config = new FabricSinkConfig(setupConfigs());
        Assertions.assertFalse(config.getEnableTableValidation());
    }

    @Test
    void shouldPerformTableValidationBasedOnParameter() {
        Arrays.asList(Boolean.valueOf("true"), Boolean.valueOf("false")).forEach(enableValidation -> {
            HashMap<String, String> settings = setupConfigs();
            settings.put(KUSTO_SINK_ENABLE_TABLE_VALIDATION, enableValidation.toString());
            FabricSinkConfig config = new FabricSinkConfig(settings);
            Assertions.assertEquals(enableValidation, config.getEnableTableValidation());
        });
    }

    @Test
    void shouldAllowNoPasswordIfManagedIdentityEnabled() {
        HashMap<String, String> settings = setupConfigs();
        settings.remove(FabricSinkConfig.KUSTO_AUTH_APPKEY_CONF);
        settings.put(FabricSinkConfig.KUSTO_AUTH_STRATEGY_CONF,
                FabricSinkConfig.KustoAuthenticationStrategy.MANAGED_IDENTITY.name().toLowerCase());
        FabricSinkConfig config = new FabricSinkConfig(settings);
        Assertions.assertNotNull(config);
    }

    @Test
    void shouldDeserializeTablesMappings() throws JsonProcessingException {
        HashMap<String, String> settings = setupConfigs();
        FabricSinkConfig config = new FabricSinkConfig(settings);
        Assertions.assertArrayEquals(
                new TopicToTableMapping[] {
                        new TopicToTableMapping(null, "csv", "table1", "db1", "topic1", false),
                        new TopicToTableMapping("Mapping", "json", "table2", "db2", "topic2", false)
                },
                config.getTopicToTableMapping());
    }

    @ParameterizedTest(name = "Test case {index} with value {0}"
            + " should throw ConfigException on missing {1} mapping")
    @CsvSource({
            "[{'topic': 'topic1', 'db': null, 'table': 'table1','format': 'csv'}]",
            "[{'topic': 'topic1', 'db': 'db1', 'table': null,'format': 'csv'}]",
            "[{'topic': null, 'db': 'db1', 'table': 'table1','format': 'csv'}]"
    })
    void shouldThrowConfigExceptionOnMissingProps() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(
                FabricSinkConfig.KUSTO_TABLES_MAPPING_CONF,
                "[{'topic': 'topic1', 'db': null, 'table': 'table1','format': 'csv'}]");

        Assertions.assertThrows(
                ConfigException.class,
                () -> invokeGetTopicToTableMapping(settings));
    }

    private void invokeGetTopicToTableMapping(HashMap<String, String> settings) throws JsonProcessingException {
        new FabricSinkConfig(settings).getTopicToTableMapping();
    }

    public static @NotNull HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(FabricSinkConfig.KUSTO_INGEST_URL_CONF, DM_URL);
        configs.put(FabricSinkConfig.KUSTO_ENGINE_URL_CONF, ENGINE_URL);
        configs.put(FabricSinkConfig.KUSTO_TABLES_MAPPING_CONF,
                "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db2', 'table': 'table2','format': 'json','mapping': 'Mapping'}]");
        configs.put(FabricSinkConfig.KUSTO_AUTH_APPID_CONF, "some-appid");
        configs.put(FabricSinkConfig.KUSTO_AUTH_APPKEY_CONF, "some-appkey");
        configs.put(FabricSinkConfig.KUSTO_AUTH_AUTHORITY_CONF, "some-authority");
        return configs;
    }
}
