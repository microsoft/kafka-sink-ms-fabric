package com.microsoft.fabric.connect.eventhouse.sink.it.containers;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.fabric.connect.eventhouse.sink.Utils;

public class EventHouseKafkaConnectContainer extends GenericContainer<EventHouseKafkaConnectContainer> {
    private static final String KAFKA_CONNECT_IMAGE = "confluentinc/cp-kafka-connect-base";

    private static final int KAFKA_CONNECT_PORT = 8083;
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHouseKafkaConnectContainer.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);

    private static final Duration KAFKA_CONNECT_START_TIMEOUT = Duration.ofMinutes(1);
    private final String kafkaListeners;

    public EventHouseKafkaConnectContainer(final String version, final String kafkaListeners) {
        super(KAFKA_CONNECT_IMAGE + ":" + version);
        this.kafkaListeners = kafkaListeners;
        waitingFor(Wait.forHttp("/connectors").forStatusCode(200));
        withExposedPorts(KAFKA_CONNECT_PORT);
    }

    public EventHouseKafkaConnectContainer withKafka(final @NotNull ConfluentKafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork());
    }

    private EventHouseKafkaConnectContainer withKafka(final Network network) {
        withNetwork(network);
        Map<String, String> env = new HashMap<>();
        env.put("BOOTSTRAP_SERVERS", this.kafkaListeners);
        env.put("CONNECT_BOOTSTRAP_SERVERS", this.kafkaListeners);
        env.put("CONNECT_GROUP_ID", "kusto-e2e-connect-group");
        env.put("CONNECT_CONFIG_STORAGE_TOPIC", "connect-config");
        env.put("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets");
        env.put("CONNECT_STATUS_STORAGE_TOPIC", "connect-status");
        env.put("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO");
        env.put("CONNECT_LOG4J_LOGGERS", "org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR");
        env.put("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        env.put("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
        env.put("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
        env.put("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        env.put("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        env.put("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        env.put("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        env.put("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter");
        env.put("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter");
        env.put("CONNECT_REST_ADVERTISED_HOST_NAME", "kusto-e2e-connect");
        env.put("CONNECT_REST_PORT", String.valueOf(KAFKA_CONNECT_PORT));
        env.put("CONNECT_PLUGIN_PATH", Utils.getConnectPath());
        env.put("CLASSPATH", Utils.getConnectPath());
        withEnv(env);
        return self();
    }

    public String getTarget() {
        return "http://" + getHost() + ":" + getMappedPort(KAFKA_CONNECT_PORT);
    }

    public void registerConnector(String name, Map<String, Object> configuration) {
        try {
            Map<String, Object> connectorConfiguration = new HashMap<>();
            connectorConfiguration.put("name", name);
            connectorConfiguration.put("config", configuration);
            String postConfig = OBJECT_MAPPER.writeValueAsString(connectorConfiguration);
            LOGGER.trace("Registering connector {} with config {}", name, postConfig);
            executePOSTRequestSuccessfully(postConfig, String.format("%s/connectors", getTarget()));
            Awaitility.await()
                    .atMost(KAFKA_CONNECT_START_TIMEOUT)
                    .until(() -> isConnectorConfigured(name));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isConnectorConfigured(String connectorName) {
        // HTTP get request to check if connector is configured
        URI connectorUri = URI.create(String.format("%s/connectors/%s/status", getTarget(), connectorName));
        HttpGet httpget = new HttpGet(connectorUri);
        try (CloseableHttpClient httpclient = HttpClients.createDefault();
                CloseableHttpResponse httpResponse = httpclient.execute(httpget)) {
            int responseCode = httpResponse.getStatusLine().getStatusCode();
            return 200 <= responseCode && responseCode <= 300;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void handleFailedResponse(HttpResponse response) throws IOException {
        String responseBody = EntityUtils.toString(response.getEntity());
        LOGGER.error("Error registering connector with error {}", responseBody);
        throw new RuntimeException("Error registering connector with error " + responseBody);
    }

    private void executePOSTRequestSuccessfully(final String payload, final String fullUrl) {
        final HttpPost httpPost = new HttpPost(URI.create(fullUrl));
        final StringEntity entity = new StringEntity(payload, StandardCharsets.UTF_8);
        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        try (CloseableHttpClient client = HttpClients.createDefault();
                CloseableHttpResponse response = client
                        .execute(httpPost)) {
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 201) {
                handleFailedResponse(response);
            }
        } catch (IOException e) {
            LOGGER.error("Error registering connector, exception when invoking endpoint {}", fullUrl, e);
        }
    }

    public String getConnectorTaskState(String connectorName, int taskNumber) {
        // HTTP get request to check if connector is configured
        URI statusUri = URI.create(String.format("%s/connectors/%s/tasks/%d/status", getTarget(), connectorName, taskNumber));
        HttpGet httpget = new HttpGet(statusUri);
        try (CloseableHttpClient httpclient = HttpClients.createDefault();
                CloseableHttpResponse httpResponse = httpclient.execute(httpget)) {
            int responseCode = httpResponse.getStatusLine().getStatusCode();
            if (200 <= responseCode && responseCode <= 300) {
                try {
                    String responseBody = EntityUtils.toString(httpResponse.getEntity());
                    Map<?, ?> responseMap = OBJECT_MAPPER.readValue(responseBody, Map.class);
                    String connectorState = (String) responseMap.get("state");
                    LOGGER.info("Connector {} task {} state is {}", connectorName, taskNumber, connectorState);
                    return connectorState;
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void waitUntilConnectorTaskStateChanges(String connectorName, int taskNumber, String status) {
        Awaitility.await()
                .atMost(KAFKA_CONNECT_START_TIMEOUT)
                .until(() -> status.equalsIgnoreCase(getConnectorTaskState(connectorName, taskNumber)));
    }
}
