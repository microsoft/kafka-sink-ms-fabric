package com.microsoft.fabric.kafka.connect.sink.es;

import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.ingest.IngestionProperties;

public class EventStreamSinkConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamSinkConfig.class);

    static final String ES_CONNECTION_STRING = "connection.string";
    static final String ES_CONNECTION_STRING_DOC = "The connection string to the EventStream target";
    static final String ES_CONNECTION_STRING_DISPLAY = "Event Stream Connection String (Connection string to the EventStream)";

    static final String ES_TARGET = "eventhub.name";
    static final String ES_TARGET_DOC = "The name of the EventHub to send the data to";
    static final String ES_TARGET_DISPLAY = "Event Stream Target (Eventhub name to write data to in Fabric)";

    static final String ES_MESSAGE_FORMAT = "message.format";
    static final String ES_MESSAGE_FORMAT_DOC = "The format of the message to be sent to the EventHub (AVRO,JSON)";
    static final String ES_MESSAGE_FORMAT_DISPLAY = "Event Stream Message Format (Supports AVRO, JSON)";

    static final String ES_CLIENT_NAME = "client.name";
    static final String ES_CLIENT_NAME_DOC = "The name of the client to be used for sending data to the EventHub";
    static final String ES_CLIENT_NAME_DISPLAY = "Event Stream Client Name";


    private String connectionString;
    private String clientName;
    private String eventHubName;
    private IngestionProperties.DataFormat messageFormat;


    public EventStreamSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public static @NotNull ConfigDef getConfig() {
        try {
            ConfigDef result = new ConfigDef();
            defineConnectionConfigs(result);
            return result;
        } catch (Exception ex) {
            LOGGER.error("Error in initializing config", ex);
            throw new RuntimeException("Error initializing config. Exception ", ex);
        }
    }

    public EventStreamSinkConfig(@NotNull Map<String, String> props) {
        this(getConfig(), props);
        connectionString = props.get(ES_CONNECTION_STRING);
        eventHubName = props.get(ES_TARGET);
        if(StringUtils.isEmpty(ES_CLIENT_NAME)) {
            LOGGER.warn("'message.format' is not provided, using JSON as the default format");
            messageFormat = IngestionProperties.DataFormat.JSON;
        } else {
            messageFormat = IngestionProperties.DataFormat.valueOf(props.get(ES_MESSAGE_FORMAT).toUpperCase(Locale.ROOT));
        }
        String clientName = props.get(ES_CLIENT_NAME);
        if(StringUtils.isEmpty(clientName)){
            this.clientName = String.format("default-%s", System.currentTimeMillis());
            LOGGER.warn("Client name is not provided, using default client name");
        } else {
            this.clientName = clientName;
        }
        LOGGER.info("Using target hub {} in format {}", eventHubName, messageFormat);
    }

    public String getConnectionString() {
        return connectionString;
    }
    public String getEventHubName() {
        return eventHubName;
    }

    public IngestionProperties.DataFormat getTargetDataFormat() {
        return messageFormat;
    }

    public Properties getEsProducerProperties(){
        if(StringUtils.isNotEmpty(connectionString)) {
            return getProperties();
        } else {
            throw new IllegalArgumentException("Connection string is empty, Initialize this with the properties " +
                    "constructor with the 'connection.string' key");
        }
    }

    private @NotNull Properties getProperties() {
        Properties props = new Properties();
        String namespace = connectionString.substring(connectionString.indexOf("/") + 2, connectionString.indexOf("."));
        props.put("bootstrap.servers", String.format("%s.servicebus.windows.net:9093", namespace));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"%s\";", connectionString));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientName);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
    /*
        static final String ES_CONNECTION_STRING = "connection.string";
    static final String ES_TARGET = "eventhub.name";
    static final String ES_MESSAGE_FORMAT = "message.format";
    static final String ES_CLIENT_NAME = "client.name";
     */

    private static void defineConnectionConfigs(@NotNull ConfigDef result) {
        final String connectionGroupName = "Connection";
        int connectionGroupOrder = 0;
        result
                .define(
                        ES_CONNECTION_STRING,
                        ConfigDef.Type.PASSWORD,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        ES_CONNECTION_STRING_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        ES_CONNECTION_STRING_DISPLAY)
                .define(
                        ES_TARGET,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        ES_TARGET_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        ES_TARGET_DISPLAY)
                .define(
                        ES_CLIENT_NAME,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        ES_CLIENT_NAME_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        ES_CLIENT_NAME_DISPLAY)
                .define(
                        ES_MESSAGE_FORMAT,
                        ConfigDef.Type.STRING,
                        IngestionProperties.DataFormat.JSON.name(),
                        ConfigDef.Importance.HIGH,
                        ES_MESSAGE_FORMAT_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        ES_MESSAGE_FORMAT_DISPLAY);
    }
}
