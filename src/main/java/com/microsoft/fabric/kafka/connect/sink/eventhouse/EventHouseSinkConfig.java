package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class EventHouseSinkConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHouseSinkConfig.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);

    private static final String EVENTHOUSE_CONNECTION_STRING = "connection.string";
    private static final String EVENTHOUSE_AUTH_STRATEGY_CONF = "aad.auth.strategy";
    private static final String EVENTHOUSE_TABLES_MAPPING_CONF = "eh.tables.topics.mapping";
    private static final String EVENTHOUSE_SINK_FLUSH_SIZE_BYTES_CONF = "eh.flush.size.bytes";
    private static final String EVENTHOUSE_SINK_FLUSH_INTERVAL_MS_CONF = "eh.flush.interval.ms";
    private static final String EVENTHOUSE_BEHAVIOR_ON_ERROR_CONF = "behavior.on.error";
    private static final String EVENTHOUSE_DLQ_BOOTSTRAP_SERVERS_CONF = "misc.deadletterqueue.bootstrap.servers";
    private static final String EVENTHOUSE_DLQ_TOPIC_NAME_CONF = "misc.deadletterqueue.topic.name";
    private static final String EVENTHOUSE_CONNECTION_PROXY_HOST = "proxy.host";
    private static final String EVENTHOUSE_CONNECTION_PROXY_PORT = "proxy.port";
    private static final String EVENTHOUSE_SINK_MAX_RETRY_TIME_MS_CONF = "errors.retry.max.time.ms";
    private static final String EVENTHOUSE_SINK_RETRY_BACKOFF_TIME_MS_CONF = "errors.retry.backoff.time.ms";
    private static final String DLQ_PROPS_PREFIX = "misc.deadletterqueue.";
    private static final String EVENTHOUSE_USE_MANAGED_IDENTITY = "eh.managed.identity";
    private static final String EVENTHOUSE_MANAGED_IDENTITY_ID = "eh.managed.identity.id";
    private static final String EVENTHOUSE_USE_WORKLOAD_IDENTITY = "eh.workload.identity";

    public EventHouseSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public EventHouseSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static @NotNull ConfigDef getConfig() {
        try {
            String tempDirectory = System.getProperty("java.io.tmpdir");
            ConfigDef result = new ConfigDef();
            defineConnectionConfigs(result);
            defineWriteConfigs(result);
            defineErrorHandlingAndRetriesConfigs(result);
            return result;
        } catch (Exception ex) {
            LOGGER.error("Error in initializing config", ex);
            throw new RuntimeException("Error initializing config. Exception ", ex);
        }
    }

    private static void defineErrorHandlingAndRetriesConfigs(@NotNull ConfigDef result) {
        final String errorAndRetriesGroupName = "Error Handling and Retries";
        int errorAndRetriesGroupOrder = 0;
        result
                .define(
                        EVENTHOUSE_BEHAVIOR_ON_ERROR_CONF,
                        ConfigDef.Type.STRING,
                        BehaviorOnError.FAIL.name(),
                        ConfigDef.ValidString.in(
                                BehaviorOnError.FAIL.name(), BehaviorOnError.LOG.name(), BehaviorOnError.IGNORE.name(),
                                BehaviorOnError.FAIL.name().toLowerCase(Locale.ENGLISH), BehaviorOnError.LOG.name().toLowerCase(Locale.ENGLISH),
                                BehaviorOnError.IGNORE.name().toLowerCase(Locale.ENGLISH)),
                        ConfigDef.Importance.LOW,
                        "Behavior on error setting for ingestion of records into Kusto table. Must be configured to one of the following:\n" +
                                "``fail``\n" +
                                "    Stops the connector when an error occurs while processing records or ingesting records in Kusto table.\n" +
                                "``ignore``\n" +
                                "    Continues to process next set of records when error occurs while processing records or ingesting records in Kusto table.\n"
                                +
                                "``log``\n" +
                                "    Logs the error message and continues to process subsequent records when an error occurs while processing records or ingesting records in Kusto table, available in connect logs.",
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        ConfigDef.Width.LONG,
                        "Behavior On Error")
                .define(
                        EVENTHOUSE_DLQ_BOOTSTRAP_SERVERS_CONF,
                        ConfigDef.Type.LIST,
                        "",
                        ConfigDef.Importance.LOW,
                        "Configure this list to Kafka broker's address(es) to which the Connector should write records failed due to restrictions while writing to the file in `tempdir.path`, network interruptions or unavailability of Kusto cluster. This list should be in the form host-1:port-1,host-2:port-2,…host-n:port-n.",
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Miscellaneous Dead-Letter Queue Bootstrap Servers")
                .define(
                        EVENTHOUSE_DLQ_TOPIC_NAME_CONF,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        "Set this to the Kafka topic's name to which the Connector should write records failed due to restrictions while writing to the file in `tempdir.path`, network interruptions or unavailability of Kusto cluster.",
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Miscellaneous Dead-Letter Queue Topic Name")
                .define(
                        EVENTHOUSE_SINK_MAX_RETRY_TIME_MS_CONF,
                        ConfigDef.Type.LONG,
                        TimeUnit.SECONDS.toMillis(300),
                        ConfigDef.Importance.LOW,
                        "Maximum time up to which the Connector should retry writing records to Kusto table in case of failures.",
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Errors Maximum Retry Time")
                .define(
                        EVENTHOUSE_SINK_RETRY_BACKOFF_TIME_MS_CONF,
                        ConfigDef.Type.LONG,
                        TimeUnit.SECONDS.toMillis(10),
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "BackOff time between retry attempts the Connector makes to ingest records into Kusto table.",
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Errors Retry BackOff Time");
    }

    private static void defineWriteConfigs(@NotNull ConfigDef result) {
        final String writeGroupName = "Writes";
        int writeGroupOrder = 0;
        result
                .define(
                        EVENTHOUSE_TABLES_MAPPING_CONF,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "A JSON array mapping ingestion from topic to table, e.g: "
                                + "[{'topic1':'t1','db':'kustoDb', 'table': 'table1', 'format': 'csv', 'mapping': 'csvMapping', 'streaming': 'false'}..].\n"
                                + "Streaming is optional, defaults to false. Mind usage and cogs of streaming ingestion, read here: https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming.\n"
                                + "Note: If the streaming ingestion fails transiently,"
                                + " queued ingest would apply for this specific batch ingestion. Batching latency is configured regularly via"
                                + "ingestion batching policy",
                        writeGroupName,
                        writeGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Kusto Table Topics Mapping")
                .define(
                        EVENTHOUSE_SINK_FLUSH_SIZE_BYTES_CONF,
                        ConfigDef.Type.LONG,
                        FileUtils.ONE_MB,
                        ConfigDef.Range.atLeast(100),
                        ConfigDef.Importance.MEDIUM,
                        "Kusto sink max buffer size (per topic+partition combination).",
                        writeGroupName,
                        writeGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Maximum Flush Size")
                .define(
                        EVENTHOUSE_SINK_FLUSH_INTERVAL_MS_CONF,
                        ConfigDef.Type.LONG,
                        TimeUnit.SECONDS.toMillis(30),
                        ConfigDef.Range.atLeast(100),
                        ConfigDef.Importance.HIGH,
                        "Kusto sink max staleness in milliseconds (per topic+partition combo).",
                        writeGroupName,
                        writeGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Maximum Flush Interval");
    }

    private static void defineConnectionConfigs(@NotNull ConfigDef result) {
        final String connectionGroupName = "Connection";
        int connectionGroupOrder = 0;
        result
                .define(
                        EVENTHOUSE_CONNECTION_STRING,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.LOW,
                        "ConnectionString to connect to an EventHouse. Refer https://learn.microsoft.com/en-us/kusto/api/connection-strings/kusto?view=microsoft-fabric for details",
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "EventHouse Connection String")
                .define(
                        EVENTHOUSE_AUTH_STRATEGY_CONF,
                        ConfigDef.Type.STRING,
                        KustoAuthenticationStrategy.APPLICATION.name(),
                        ConfigDef.ValidString.in(
                                KustoAuthenticationStrategy.APPLICATION.name(),
                                KustoAuthenticationStrategy.APPLICATION.name().toLowerCase(Locale.ENGLISH),
                                KustoAuthenticationStrategy.MANAGED_IDENTITY.name(),
                                KustoAuthenticationStrategy.MANAGED_IDENTITY.name().toLowerCase(Locale.ENGLISH),
                                KustoAuthenticationStrategy.AZ_DEV_TOKEN.name(),
                                KustoAuthenticationStrategy.AZ_DEV_TOKEN.name().toLowerCase(Locale.ENGLISH),
                                KustoAuthenticationStrategy.WORKLOAD_IDENTITY.name(),
                                KustoAuthenticationStrategy.WORKLOAD_IDENTITY.name().toLowerCase(Locale.ENGLISH)),
                        ConfigDef.Importance.HIGH,
                        "Strategy to authenticate against Azure Active Directory, either ``application`` (default) or ``managed_identity``.",
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Kusto Auth Strategy")
                .define(
                        EVENTHOUSE_CONNECTION_PROXY_HOST,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        "Proxy host",
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Proxy host used to connect to EventHouse")
                .define(
                        EVENTHOUSE_CONNECTION_PROXY_PORT,
                        ConfigDef.Type.INT,
                        -1,
                        ConfigDef.Importance.LOW,
                        "Proxy port",
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Proxy port used to connect to EventHouse")
                .define(
                        EVENTHOUSE_USE_MANAGED_IDENTITY,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        "Use managed identity",
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Use managed identity to connect to EventHouse")
                .define(
                        EVENTHOUSE_MANAGED_IDENTITY_ID,
                        ConfigDef.Type.STRING,
                        "system",
                        ConfigDef.Importance.LOW,
                        "Use managed identity",
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Use managed identity to connect to EventHouse")
                .define(
                        EVENTHOUSE_USE_WORKLOAD_IDENTITY,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        "Use workload identity",
                        connectionGroupName,
                        connectionGroupOrder++,
                        ConfigDef.Width.MEDIUM,
                        "Use workload identity to connect to EventHouse");
    }

    public String getEventhouseConnectionString() {
        return this.getString(EVENTHOUSE_CONNECTION_STRING);
    }

    public boolean isManagedIdentity() {
        return this.getBoolean(EVENTHOUSE_USE_MANAGED_IDENTITY);
    }

    public boolean isWorkloadIdentity() {
        return this.getBoolean(EVENTHOUSE_USE_WORKLOAD_IDENTITY);
    }

    public KustoAuthenticationStrategy getAuthStrategy() {
        return KustoAuthenticationStrategy.valueOf(getString(EVENTHOUSE_AUTH_STRATEGY_CONF).toUpperCase(Locale.ENGLISH));
    }

    public String getRawTopicToTableMapping() {
        return getString(EVENTHOUSE_TABLES_MAPPING_CONF);
    }

    public TopicToTableMapping[] getTopicToTableMapping() {
        try {
            TopicToTableMapping[] mappings = OBJECT_MAPPER.readValue(getRawTopicToTableMapping(), TopicToTableMapping[].class);
            for (TopicToTableMapping mapping : mappings) {
                mapping.validate();
            }
            return mappings;
        } catch (JsonProcessingException e) {
            throw new ConfigException("Error parsing topic to table mapping", e);
        }
    }

    public long getFlushSizeBytes() {
        return getLong(EVENTHOUSE_SINK_FLUSH_SIZE_BYTES_CONF);
    }

    public long getFlushInterval() {
        return getLong(EVENTHOUSE_SINK_FLUSH_INTERVAL_MS_CONF);
    }

    public BehaviorOnError getBehaviorOnError() {
        return BehaviorOnError.valueOf(
                getString(EVENTHOUSE_BEHAVIOR_ON_ERROR_CONF).toUpperCase(Locale.ENGLISH));
    }

    public boolean isDlqEnabled() {
        if (!getDlqBootstrapServers().isEmpty() && StringUtils.isNotEmpty(getDlqTopicName())) {
            return true;
        } else if (getDlqBootstrapServers().isEmpty() && StringUtils.isEmpty(getDlqTopicName())) {
            return false;
        } else {
            throw new ConfigException("To enable Miscellaneous Dead-Letter Queue configuration please configure both " +
                    "`misc.deadletterqueue.bootstrap.servers` and `misc.deadletterqueue.topic.name` configurations ");
        }
    }

    public List<String> getDlqBootstrapServers() {
        return this.getList(EVENTHOUSE_DLQ_BOOTSTRAP_SERVERS_CONF);
    }

    public String getDlqTopicName() {
        return getString(EVENTHOUSE_DLQ_TOPIC_NAME_CONF);
    }

    public String getConnectionProxyHost() {
        return getString(EVENTHOUSE_CONNECTION_PROXY_HOST);
    }

    public Integer getConnectionProxyPort() {
        return getInt(EVENTHOUSE_CONNECTION_PROXY_PORT);
    }

    public Properties getDlqProps() {
        Map<String, Object> dlqconfigs = originalsWithPrefix(DLQ_PROPS_PREFIX);
        Properties props = new Properties();
        props.putAll(dlqconfigs);
        props.put("bootstrap.servers", getDlqBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return props;
    }

    public long getMaxRetryAttempts() {
        return this.getLong(EVENTHOUSE_SINK_MAX_RETRY_TIME_MS_CONF)
                / this.getLong(EVENTHOUSE_SINK_RETRY_BACKOFF_TIME_MS_CONF);
    }

    public long getRetryBackOffTimeMs() {
        return this.getLong(EVENTHOUSE_SINK_RETRY_BACKOFF_TIME_MS_CONF);
    }

    public enum BehaviorOnError {
        FAIL, LOG, IGNORE;

        /**
         * Gets names of available behavior on error mode.
         *
         * @return array of available behavior on error mode names
         */
        public static String @NotNull [] getNames() {
            return Arrays
                    .stream(BehaviorOnError.class.getEnumConstants())
                    .map(Enum::name)
                    .toArray(String[]::new);
        }
    }

    public enum KustoAuthenticationStrategy {
        APPLICATION, MANAGED_IDENTITY, AZ_DEV_TOKEN, WORKLOAD_IDENTITY
    }
}
