package com.microsoft.fabric.kafka.connect.sink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.fabric.kafka.connect.sink.eventhouse.EventHouseSinkConfig;
import com.microsoft.fabric.kafka.connect.sink.eventhouse.TopicToTableMapping;

import static org.junit.jupiter.api.Assertions.fail;

@Disabled
public class FabricSinkConnectorConfigTest {
    private static final String DM_URL = "https://ingest-cluster_name.kusto.windows.net";
    private static final String ENGINE_URL = "https://cluster_name.kusto.windows.net";

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        EventHouseSinkConfig config = new EventHouseSinkConfig(setupConfigs());
        Assertions.assertNotNull(config);
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        EventHouseSinkConfig config = new EventHouseSinkConfig(setupConfigs());
        Assertions.assertNotNull(config.getConnectionString());
        Assertions.assertTrue(config.getFlushSizeBytes() > 0);
        Assertions.assertTrue(config.getFlushInterval() > 0);
        Assertions.assertFalse(config.isDlqEnabled());
        Assertions.assertEquals(EventHouseSinkConfig.BehaviorOnError.FAIL, config.getBehaviorOnError());
    }

    @Test
    public void shouldThrowExceptionWhenKustoURLNotGiven() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove("kusto.ingestion.url");
        Assertions.assertThrows(ConfigException.class, () -> new EventHouseSinkConfig(settings));
    }

    @Test
    public void shouldFailWhenBehaviorOnErrorIsIllConfigured() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove("kusto.ingestion.url");
        settings.put("behavior.on.error", "DummyValue");
        Assertions.assertThrows(ConfigException.class, () -> new EventHouseSinkConfig(settings));
    }

    @Test
    public void verifyDlqSettings() {
        HashMap<String, String> settings = setupConfigs();
        settings.put("misc.deadletterqueue.bootstrap.servers", "localhost:8081,localhost:8082");
        settings.put("misc.deadletterqueue.topic.name", "dlq-error-topic");
        EventHouseSinkConfig config = new EventHouseSinkConfig(settings);
        Assertions.assertTrue(config.isDlqEnabled());
        Assertions.assertEquals(Arrays.asList("localhost:8081", "localhost:8082"), config.getDlqBootstrapServers());
        Assertions.assertEquals("dlq-error-topic", config.getDlqTopicName());
    }

    @Test
    public void shouldProcessDlqConfigsWithPrefix() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.put("misc.deadletterqueue.security.protocol", "SASL_PLAINTEXT");
        settings.put("misc.deadletterqueue.sasl.mechanism", "PLAIN");
        try {
            EventHouseSinkConfig config = new EventHouseSinkConfig(settings);
            Assertions.assertNotNull(config);
            Properties dlqProps = config.getDlqProps();
            Assertions.assertEquals("SASL_PLAINTEXT", dlqProps.get("security.protocol"));
            Assertions.assertEquals("PLAIN", dlqProps.get("sasl.mechanism"));
        } catch (Exception ex) {
            fail(ex);
        }
    }

    @Test
    public void shouldNotPerformTableValidationByDefault() {
        EventHouseSinkConfig config = new EventHouseSinkConfig(setupConfigs());
        Assertions.assertFalse(config.getEnableTableValidation());
    }

    @Test
    public void shouldPerformTableValidationBasedOnParameter() {
        Arrays.asList(Boolean.valueOf("true"), Boolean.valueOf("false")).forEach(enableValidation -> {
            HashMap<String, String> settings = setupConfigs();
            settings.put("kusto.validation.table.enable", enableValidation.toString());
            EventHouseSinkConfig config = new EventHouseSinkConfig(settings);
            Assertions.assertEquals(enableValidation, config.getEnableTableValidation());
        });
    }

    @Test
    public void shouldAllowNoPasswordIfManagedIdentityEnabled() {
        HashMap<String, String> settings = setupConfigs();
        settings.remove("aad.auth.appkey");
        settings.put("aad.auth.strategy","managed_identity");
        EventHouseSinkConfig config = new EventHouseSinkConfig(settings);
        Assertions.assertNotNull(config);
    }

    @Test
    public void shouldDeserializeTablesMappings() throws JsonProcessingException {
        HashMap<String, String> settings = setupConfigs();
        EventHouseSinkConfig config = new EventHouseSinkConfig(settings);
        Assertions.assertArrayEquals(
                new TopicToTableMapping[] {
                        new TopicToTableMapping(null, "csv", "table1", "db1", "topic1", false),
                        new TopicToTableMapping("Mapping", "json", "table2", "db2", "topic2", false)
                },
                config.getTopicToTableMapping());
    }

    @Test
    public void shouldThrowConfigExceptionOnMissingTopicMapping() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(
                "kusto.tables.topics.mapping",
                "[{'topic': null, 'db': 'db1', 'table': 'table1','format': 'csv'}]");

        Assertions.assertThrows(
                ConfigException.class,
                () -> new EventHouseSinkConfig(settings).getTopicToTableMapping());
    }

    @Test
    public void shouldThrowConfigExceptionOnMissingDbMapping() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(
                "kusto.tables.topics.mapping",
                "[{'topic': 'topic1', 'db': null, 'table': 'table1','format': 'csv'}]");

        Assertions.assertThrows(
                ConfigException.class,
                () -> new EventHouseSinkConfig(settings).getTopicToTableMapping());
    }

    @Test
    public void shouldThrowConfigExceptionOnMissingTableMapping() {
        HashMap<String, String> settings = setupConfigs();
        settings.put(
                "kusto.tables.topics.mapping",
                "[{'topic': 'topic1', 'db': 'db1', 'table': null,'format': 'csv'}]");

        Assertions.assertThrows(
                ConfigException.class,
                () -> new EventHouseSinkConfig(settings).getTopicToTableMapping());
    }

    public static HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put("kusto.ingestion.url", DM_URL);
        configs.put("kusto.query.url", ENGINE_URL);
        configs.put("kusto.tables.topics.mapping",
                "[{'topic': 'topic1','db': 'db1', 'table': 'table1','format': 'csv'},{'topic': 'topic2','db': 'db2', 'table': 'table2','format': 'json','mapping': 'Mapping'}]");
        configs.put("aad.auth.appid", "some-appid");
        configs.put("aad.auth.appkey", "some-appkey");
        configs.put("aad.auth.authority", "some-authority");
        return configs;
    }
}
