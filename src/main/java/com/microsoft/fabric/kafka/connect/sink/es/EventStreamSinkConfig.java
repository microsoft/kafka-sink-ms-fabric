package com.microsoft.fabric.kafka.connect.sink.es;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

public class EventStreamSinkConfig extends AbstractConfig {
    static final String ES_CONNECTION_STRING = "connection.string";
    static final String ES_CONNECTION_STRING_DISPLAY = "Event Stream Connection String (Connection string to the EventStream)";
    static final String ES_MESSAGE_FORMAT = "es.message.format";
    static final String ES_MESSAGE_FORMAT_DISPLAY = "Event Stream Message Format (Supports AVRO, JSON)";
    /**
     * Each client will create its own TCP connection, which helps with gaining more throughput
     */
    static final String ES_CLIENTS_PER_TASK = "es.clients.per.task";
    static final String ES_CLIENTS_PER_TASK_DOC = "Number of Event stream clients to use per task";
    static final String ES_CLIENTS_PER_TASK_DISPLAY = "Clients per task";
    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(
                    ES_CONNECTION_STRING,
                    ConfigDef.Type.PASSWORD,
                    ConfigDef.Importance.HIGH,
                    ES_CONNECTION_STRING_DISPLAY)
            .define(
                    ES_MESSAGE_FORMAT,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    ES_MESSAGE_FORMAT_DISPLAY)
            .define(
                    ES_CLIENTS_PER_TASK,
                    ConfigDef.Type.SHORT,
                    1,
                    ConfigDef.Importance.LOW,
                    ES_CLIENTS_PER_TASK_DOC,
                    ES_CLIENTS_PER_TASK_DISPLAY);
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamSinkConfig.class);

    public EventStreamSinkConfig(@NotNull Map<String, String> configValues) {
        super(CONFIG_DEF, configValues);
        String connectionString = configValues.get(ES_CONNECTION_STRING);
        IngestionProperties.DataFormat messageFormat = IngestionProperties.DataFormat.JSON;
        if (StringUtils.isEmpty(ES_MESSAGE_FORMAT)) {
            LOGGER.warn("'message.format' is not provided, using JSON as the default format");
        } else {
            messageFormat = IngestionProperties.DataFormat.valueOf(configValues.get(ES_MESSAGE_FORMAT).toUpperCase(Locale.ROOT));
        }
        LOGGER.info("Using target hub {} in format {}", EventStreamCommon.getSecureConnectionString(connectionString), messageFormat);
    }
}
