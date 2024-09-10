package com.microsoft.fabric.kafka.connect.sink.eventhouse;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class EventHouseSinkConfig extends AbstractConfig {
    private static final String DLQ_PROPS_PREFIX = "misc.deadletterqueue.";
    private static final String KQL_DB_CONNECTION_STRING = "connection.string";
    private static final String KQL_DB_CONNECTION_STRING_DOC = "The connection string to the KQL DB event house";
    private static final String KUSTO_AUTH_ACCESS_TOKEN_DOC = "Kusto Access Token for Azure Active Directory authentication";
    private static final String KUSTO_AUTH_STRATEGY_DOC = "Strategy to authenticate against Azure Active Directory, either ``application`` (default) or ``managed_identity``.";
    private static final String KUSTO_BEHAVIOR_ON_ERROR_CONF = "behavior.on.error";
    private static final String KUSTO_CONNECTION_PROXY_HOST = "proxy.host";
    private static final String KUSTO_CONNECTION_PROXY_HOST_DOC = "Proxy host";
    private static final String KUSTO_CONNECTION_PROXY_PORT = "proxy.port";
    private static final String KUSTO_CONNECTION_PROXY_PORT_DOC = "Proxy port";
    private static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF = "misc.deadletterqueue.bootstrap.servers";
    private static final String KUSTO_DLQ_TOPIC_NAME_CONF = "misc.deadletterqueue.topic.name";
    private static final String KUSTO_SINK_ENABLE_TABLE_VALIDATION_DOC = "Enable table access validation at task start.";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DOC = "Kusto sink max staleness in milliseconds (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DOC = "Kusto sink max buffer size (per topic+partition combo).";
    private static final String KUSTO_SINK_MAX_RETRY_TIME_MS_CONF = "errors.retry.max.time.ms";
    private static final String KUSTO_AUTH_ACCESS_TOKEN_CONF = "aad.auth.accesstoken";
    private static final String KUSTO_AUTH_STRATEGY_CONF = "aad.auth.strategy";
    private static final String KUSTO_SINK_ENABLE_TABLE_VALIDATION = "kusto.validation.table.enable";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_CONF = "flush.interval.ms";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_CONF = "flush.size.bytes";
    private static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF = "errors.retry.backoff.time.ms";
    private static final String KUSTO_SINK_TEMP_DIR_CONF = "tempdir.path";
    private static final String KUSTO_TABLES_MAPPING_CONF = "kusto.tables.topics.mapping";

    private static final String KUSTO_SINK_TEMP_DIR_DOC = "Temp dir that will be used by kusto sink to buffer records. "
            + "defaults to system temp dir.";

    private static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DOC = "BackOff time between retry attempts "
            + "the Connector makes to ingest records into Kusto table.";

    private static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_DOC = "Configure this list to Kafka broker's address(es) "
            + "to which the Connector should write records failed due to restrictions while writing to the file in `tempdir.path`, network interruptions or unavailability of Kusto cluster. "
            + "This list should be in the form host-1:port-1,host-2:port-2,…host-n:port-n.";
    private static final String KUSTO_BEHAVIOR_ON_ERROR_DOC = "Behavior on error setting for "
            + "ingestion of records into Kusto table. "
            + "Must be configured to one of the following:\n"
            + "``fail``\n"
            + "    Stops the connector when an error occurs "
            + "while processing records or ingesting records in Kusto table.\n"
            + "``ignore``\n"
            + "    Continues to process next set of records "
            + "when error occurs while processing records or ingesting records in Kusto table.\n"
            + "``log``\n"
            + "    Logs the error message and continues to process subsequent records when an error occurs "
            + "while processing records or ingesting records in Kusto table, available in connect logs.";
    private static final String KUSTO_TABLES_MAPPING_DOC = "A JSON array mapping ingestion from topic to table, e.g: "
            + "[{'topic1':'t1','db':'kustoDb', 'table': 'table1', 'format': 'csv', 'mapping': 'csvMapping', 'streaming': 'false'}..].\n"
            + "Streaming is optional, defaults to false. Mind usage and cogs of streaming ingestion, read here: https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming.\n"
            + "Note: If the streaming ingestion fails transiently,"
            + " queued ingest would apply for this specific batch ingestion. Batching latency is configured regularly via"
            + "ingestion batching policy";
    private static final String KUSTO_DLQ_TOPIC_NAME_DOC = "Set this to the Kafka topic's name "
            + "to which the Connector should write records failed due to restrictions while writing to the file in `tempdir.path`, network interruptions or unavailability of Kusto cluster.";
    private static final String KUSTO_SINK_MAX_RETRY_TIME_MS_DOC = "Maximum time up to which the Connector "
            + "should retry writing records to Kusto table in case of failures.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    KQL_DB_CONNECTION_STRING,
                    Type.PASSWORD,
                    ConfigDef.NO_DEFAULT_VALUE,
                    Importance.LOW,
                    KQL_DB_CONNECTION_STRING_DOC)
            .define(
                    KUSTO_AUTH_ACCESS_TOKEN_CONF,
                    Type.PASSWORD,
                    null,
                    Importance.LOW,
                    KUSTO_AUTH_ACCESS_TOKEN_DOC)
            .define(
                    KUSTO_SINK_ENABLE_TABLE_VALIDATION,
                    Type.BOOLEAN,
                    Boolean.FALSE,
                    Importance.LOW,
                    KUSTO_SINK_ENABLE_TABLE_VALIDATION_DOC)
            .define(
                    KUSTO_AUTH_STRATEGY_CONF,
                    Type.STRING,
                    KustoAuthenticationStrategy.APPLICATION.name(),
                    ConfigDef.ValidString.in(
                            KustoAuthenticationStrategy.APPLICATION.name(),
                            KustoAuthenticationStrategy.APPLICATION.name().toLowerCase(Locale.ENGLISH),
                            KustoAuthenticationStrategy.AZ_DEV_TOKEN.name(),
                            KustoAuthenticationStrategy.AZ_DEV_TOKEN.name().toLowerCase(Locale.ENGLISH)),
                    Importance.HIGH,
                    KUSTO_AUTH_STRATEGY_DOC)
            .define(
                    KUSTO_CONNECTION_PROXY_HOST,
                    Type.STRING,
                    null,
                    Importance.LOW,
                    KUSTO_CONNECTION_PROXY_HOST_DOC)
            .define(
                    KUSTO_CONNECTION_PROXY_PORT,
                    Type.INT,
                    -1,
                    Importance.LOW,
                    KUSTO_CONNECTION_PROXY_PORT_DOC)
            .define(
                    KUSTO_BEHAVIOR_ON_ERROR_CONF,
                    Type.STRING,
                    BehaviorOnError.FAIL.name(),
                    ConfigDef.ValidString.in(
                            BehaviorOnError.FAIL.name(), BehaviorOnError.LOG.name(),
                            BehaviorOnError.IGNORE.name(),
                            BehaviorOnError.FAIL.name().toLowerCase(Locale.ENGLISH),
                            BehaviorOnError.LOG.name().toLowerCase(Locale.ENGLISH),
                            BehaviorOnError.IGNORE.name().toLowerCase(Locale.ENGLISH)),
                    Importance.LOW,
                    KUSTO_BEHAVIOR_ON_ERROR_DOC)
            .define(
                    KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF,
                    Type.LIST,
                    "",
                    Importance.LOW,
                    KUSTO_DLQ_BOOTSTRAP_SERVERS_DOC)
            .define(
                    KUSTO_DLQ_TOPIC_NAME_CONF,
                    Type.STRING,
                    "",
                    Importance.LOW,
                    KUSTO_DLQ_TOPIC_NAME_DOC)
            .define(
                    KUSTO_SINK_MAX_RETRY_TIME_MS_CONF,
                    Type.LONG,
                    TimeUnit.SECONDS.toMillis(300),
                    Importance.LOW,
                    KUSTO_SINK_MAX_RETRY_TIME_MS_DOC)
            .define(
                    KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF,
                    Type.LONG,
                    TimeUnit.SECONDS.toMillis(10),
                    ConfigDef.Range.atLeast(1),
                    Importance.LOW,
                    KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DOC)
            .define(
                    KUSTO_TABLES_MAPPING_CONF,
                    Type.STRING,
                    "",
                    Importance.HIGH,
                    KUSTO_TABLES_MAPPING_DOC)
            .define(
                    KUSTO_SINK_TEMP_DIR_CONF,
                    Type.STRING,
                    System.getProperty("java.io.tmpdir"),
                    Importance.LOW,
                    KUSTO_SINK_TEMP_DIR_DOC)
            .define(
                    KUSTO_SINK_FLUSH_SIZE_BYTES_CONF,
                    Type.LONG,
                    FileUtils.ONE_MB,
                    ConfigDef.Range.atLeast(100),
                    Importance.MEDIUM,
                    KUSTO_SINK_FLUSH_SIZE_BYTES_DOC)
            .define(
                    KUSTO_SINK_FLUSH_INTERVAL_MS_CONF,
                    Type.LONG,
                    TimeUnit.SECONDS.toMillis(30),
                    ConfigDef.Range.atLeast(100),
                    Importance.HIGH,
                    KUSTO_SINK_FLUSH_INTERVAL_MS_DOC);
    private static final Logger log = LoggerFactory.getLogger(EventHouseSinkConfig.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);

    public EventHouseSinkConfig(Map<String, String> parsedConfig) {
        super(CONFIG_DEF, parsedConfig);
    }

    public String getAuthAccessToken() {
        return this.getPassword(KUSTO_AUTH_ACCESS_TOKEN_CONF).value();
    }

    public KustoAuthenticationStrategy getAuthStrategy() {
        return KustoAuthenticationStrategy.valueOf(getString(KUSTO_AUTH_STRATEGY_CONF).toUpperCase(Locale.ENGLISH));
    }

    public String getConnectionString() {
        return this.getPassword(KQL_DB_CONNECTION_STRING).value();
    }

    public String getRawTopicToTableMapping() {
        return getString(KUSTO_TABLES_MAPPING_CONF);
    }

    public TopicToTableMapping[] getTopicToTableMapping() throws JsonProcessingException {
        TopicToTableMapping[] mappings = objectMapper.readValue(getRawTopicToTableMapping(), TopicToTableMapping[].class);

        for (TopicToTableMapping mapping : mappings) {
            mapping.validate();
        }

        return mappings;
    }

    public String getTempDirPath() {
        return getString(KUSTO_SINK_TEMP_DIR_CONF);
    }

    public long getFlushSizeBytes() {
        return getLong(KUSTO_SINK_FLUSH_SIZE_BYTES_CONF);
    }

    public long getFlushInterval() {
        return getLong(KUSTO_SINK_FLUSH_INTERVAL_MS_CONF);
    }

    public BehaviorOnError getBehaviorOnError() {
        return BehaviorOnError.valueOf(
                getString(KUSTO_BEHAVIOR_ON_ERROR_CONF).toUpperCase(Locale.ENGLISH));
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
        return this.getList(KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF);
    }

    public String getDlqTopicName() {
        return getString(KUSTO_DLQ_TOPIC_NAME_CONF);
    }

    public String getConnectionProxyHost() {
        return getString(KUSTO_CONNECTION_PROXY_HOST);
    }

    public Integer getConnectionProxyPort() {
        return getInt(KUSTO_CONNECTION_PROXY_PORT);
    }

    public Properties getDlqProps() {
        Map<String, Object> dlqConfigs = originalsWithPrefix(DLQ_PROPS_PREFIX);
        Properties props = new Properties();
        props.putAll(dlqConfigs);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getDlqBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    public long getMaxRetryAttempts() {
        return this.getLong(KUSTO_SINK_MAX_RETRY_TIME_MS_CONF)
                / this.getLong(KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF);
    }

    public long getRetryBackOffTimeMs() {
        return this.getLong(KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF);
    }

    public boolean getEnableTableValidation() {
        return this.getBoolean(KUSTO_SINK_ENABLE_TABLE_VALIDATION);
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
        APPLICATION, AZ_DEV_TOKEN
    }
}
