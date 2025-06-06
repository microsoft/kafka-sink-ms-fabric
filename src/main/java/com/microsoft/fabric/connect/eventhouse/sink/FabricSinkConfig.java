package com.microsoft.fabric.connect.eventhouse.sink;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

public class FabricSinkConfig extends AbstractConfig {
    static final String CONNECTION_STRING = "connection.string";
    static final String KUSTO_INGEST_URL_CONF = "kusto.ingestion.url";
    static final String KUSTO_ENGINE_URL_CONF = "kusto.query.url";
    static final String KUSTO_AUTH_APPID_CONF = "aad.auth.appid";
    static final String KUSTO_AUTH_ACCESS_TOKEN_CONF = "aad.auth.accesstoken";
    static final String KUSTO_AUTH_APPKEY_CONF = "aad.auth.appkey";
    static final String KUSTO_AUTH_AUTHORITY_CONF = "aad.auth.authority";
    static final String KUSTO_AUTH_STRATEGY_CONF = "aad.auth.strategy";
    static final String KUSTO_TABLES_MAPPING_CONF = "kusto.tables.topics.mapping";
    static final String KUSTO_SINK_TEMP_DIR_CONF = "tempdir.path";
    static final String KUSTO_SINK_FLUSH_SIZE_BYTES_CONF = "flush.size.bytes";
    static final String KUSTO_SINK_FLUSH_INTERVAL_MS_CONF = "flush.interval.ms";
    static final String KUSTO_BEHAVIOR_ON_ERROR_CONF = "behavior.on.error";
    static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF = "misc.deadletterqueue.bootstrap.servers";
    static final String KUSTO_DLQ_TOPIC_NAME_CONF = "misc.deadletterqueue.topic.name";
    static final String KUSTO_CONNECTION_PROXY_HOST = "proxy.host";
    static final String KUSTO_CONNECTION_PROXY_PORT = "proxy.port";
    static final String KUSTO_SINK_MAX_RETRY_TIME_MS_CONF = "errors.retry.max.time.ms";
    static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF = "errors.retry.backoff.time.ms";
    static final String KUSTO_SINK_ENABLE_TABLE_VALIDATION = "kusto.validation.table.enable";
    private static final String DLQ_PROPS_PREFIX = "misc.deadletterqueue.";

    static final String HEADERS_TO_PROJECT = "headers.to.project";
    static final String HEADERS_TO_DROP = "headers.to.drop";

    private static final String CONNECTION_STRING_DOC = "Connection string for the sink. Can be an " +
            "EventStream connection string or a Kusto connection string.";
    private static final String KUSTO_INGEST_URL_DOC = "Kusto ingestion endpoint URL.";
    private static final String KUSTO_INGEST_URL_DISPLAY = "Kusto cluster ingestion URL";
    private static final String KUSTO_ENGINE_URL_DOC = "Kusto query endpoint URL.";
    private static final String KUSTO_ENGINE_URL_DISPLAY = "Kusto cluster query URL";
    private static final String KUSTO_AUTH_APPID_DOC = "Application Id for Azure Active Directory authentication.";
    private static final String KUSTO_AUTH_APPID_DISPLAY = "Kusto Auth AppID";
    private static final String KUSTO_AUTH_APPKEY_DOC = "Application Key for Azure Active Directory authentication.";
    private static final String KUSTO_CONNECTION_PROXY_HOST_DOC = "Proxy host";
    private static final String KUSTO_CONNECTION_PROXY_HOST_DISPLAY = "Proxy host used to connect to Kusto";
    private static final String KUSTO_CONNECTION_PROXY_PORT_DOC = "Proxy port";
    private static final String KUSTO_CONNECTION_PROXY_PORT_DISPLAY = "Proxy port used to connect to Kusto";

    private static final String CONNECTION_STRING_DISPLAY = "Connection string for the sink.";
    private static final String KUSTO_AUTH_APPKEY_DISPLAY = "Kusto Auth AppKey";
    private static final String KUSTO_AUTH_ACCESS_TOKEN_DISPLAY = "Kusto Auth AccessToken";
    private static final String KUSTO_AUTH_ACCESS_TOKEN_DOC = "Kusto Access Token for Azure Active Directory authentication";
    private static final String KUSTO_AUTH_AUTHORITY_DOC = "Azure Active Directory tenant.";
    private static final String KUSTO_AUTH_AUTHORITY_DISPLAY = "Kusto Auth Authority";
    private static final String KUSTO_AUTH_STRATEGY_DOC = "Strategy to authenticate against Azure Active Directory, either ``application`` (default) or ``managed_identity``.";
    private static final String KUSTO_AUTH_STRATEGY_DISPLAY = "Kusto Auth Strategy";
    private static final String KUSTO_TABLES_MAPPING_DOC = "A JSON array mapping ingestion from topic to table, e.g: "
            + "[{'topic1':'t1','db':'kustoDb', 'table': 'table1', 'format': 'csv', 'mapping': 'csvMapping', 'streaming': 'false'}..].\n"
            + "Streaming is optional, defaults to false. Mind usage and cogs of streaming ingestion, read here: https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming.\n"
            + "Note: If the streaming ingestion fails transiently,"
            + " queued ingest would apply for this specific batch ingestion. Batching latency is configured regularly via"
            + "ingestion batching policy";
    private static final String KUSTO_TABLES_MAPPING_DISPLAY = "Kusto Table Topics Mapping";
    private static final String KUSTO_SINK_TEMP_DIR_DOC = "Temp dir that will be used by kusto sink to buffer records. "
            + "defaults to system temp dir.";
    private static final String KUSTO_SINK_TEMP_DIR_DISPLAY = "Temporary Directory";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DOC = "Kusto sink max buffer size (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_SIZE_BYTES_DISPLAY = "Maximum Flush Size";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DOC = "Kusto sink max staleness in milliseconds (per topic+partition combo).";
    private static final String KUSTO_SINK_FLUSH_INTERVAL_MS_DISPLAY = "Maximum Flush Interval";
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
    private static final String KUSTO_BEHAVIOR_ON_ERROR_DISPLAY = "Behavior On Error";
    private static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_DOC = "Configure this list to Kafka broker's address(es) "
            + "to which the Connector should write records failed due to restrictions while writing to the file in `tempdir.path`, network interruptions or unavailability of Kusto cluster. "
            + "This list should be in the form host-1:port-1,host-2:port-2,…host-n:port-n.";
    private static final String KUSTO_DLQ_BOOTSTRAP_SERVERS_DISPLAY = "Miscellaneous Dead-Letter Queue Bootstrap Servers";
    private static final String KUSTO_DLQ_TOPIC_NAME_DOC = "Set this to the Kafka topic's name "
            + "to which the Connector should write records failed due to restrictions while writing to the file in `tempdir.path`, network interruptions or unavailability of Kusto cluster.";
    private static final String KUSTO_DLQ_TOPIC_NAME_DISPLAY = "Miscellaneous Dead-Letter Queue Topic Name";
    private static final String KUSTO_SINK_MAX_RETRY_TIME_MS_DOC = "Maximum time up to which the Connector "
            + "should retry writing records to Kusto table in case of failures.";
    private static final String KUSTO_SINK_MAX_RETRY_TIME_MS_DISPLAY = "Errors Maximum Retry Time";
    private static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DOC = "BackOff time between retry attempts "
            + "the Connector makes to ingest records into Kusto table.";
    private static final String KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DISPLAY = "Errors Retry BackOff Time";
    private static final String KUSTO_SINK_ENABLE_TABLE_VALIDATION_DOC = "Enable table access validation at task start.";
    private static final String KUSTO_SINK_ENABLE_TABLE_VALIDATION_DISPLAY = "Enable table validation";

    private static final String HEADERS_TO_PROJECT_DOC = "Headers to project";
    private static final String HEADERS_TO_PROJECT_DISPLAY = "Headers to be selected";

    private static final String HEADERS_TO_DROP_DOC = "Headers to drop";
    private static final String HEADERS_TO_DROP_DISPLAY = "Headers to be dropped";

    private final ConcurrentHashMap<String, TopicToTableMapping> topicToTableMappingProperties = new ConcurrentHashMap<>();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);

    public FabricSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FabricSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static @NotNull ConfigDef getConfig() {
        try {
            String tempDirectory = System.getProperty("java.io.tmpdir");
            ConfigDef result = new ConfigDef();
            defineConnectionConfigs(result);
            defineWriteConfigs(result, tempDirectory);
            defineErrorHandlingAndRetriesConfigs(result);
            return result;
        } catch (Exception ex) {
            throw new ConfigException("Error initializing config. Exception ", ex);
        }
    }

    private static void defineErrorHandlingAndRetriesConfigs(@NotNull ConfigDef result) {
        final String errorAndRetriesGroupName = "Error Handling and Retries";
        int errorAndRetriesGroupOrder = 0;
        result
                .define(
                        KUSTO_BEHAVIOR_ON_ERROR_CONF,
                        Type.STRING,
                        BehaviorOnError.FAIL.name(),
                        ConfigDef.ValidString.in(
                                BehaviorOnError.FAIL.name(), BehaviorOnError.LOG.name(), BehaviorOnError.IGNORE.name(),
                                BehaviorOnError.FAIL.name().toLowerCase(Locale.ENGLISH), BehaviorOnError.LOG.name().toLowerCase(Locale.ENGLISH),
                                BehaviorOnError.IGNORE.name().toLowerCase(Locale.ENGLISH)),
                        Importance.LOW,
                        KUSTO_BEHAVIOR_ON_ERROR_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.LONG,
                        KUSTO_BEHAVIOR_ON_ERROR_DISPLAY)
                .define(
                        KUSTO_DLQ_BOOTSTRAP_SERVERS_CONF,
                        Type.LIST,
                        "",
                        Importance.LOW,
                        KUSTO_DLQ_BOOTSTRAP_SERVERS_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_DLQ_BOOTSTRAP_SERVERS_DISPLAY)
                .define(
                        KUSTO_DLQ_TOPIC_NAME_CONF,
                        Type.STRING,
                        "",
                        Importance.LOW,
                        KUSTO_DLQ_TOPIC_NAME_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_DLQ_TOPIC_NAME_DISPLAY)
                .define(
                        KUSTO_SINK_MAX_RETRY_TIME_MS_CONF,
                        Type.LONG,
                        TimeUnit.SECONDS.toMillis(10),
                        Importance.LOW,
                        KUSTO_SINK_MAX_RETRY_TIME_MS_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_SINK_MAX_RETRY_TIME_MS_DISPLAY)
                .define(
                        KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF,
                        Type.LONG,
                        TimeUnit.SECONDS.toMillis(2),
                        ConfigDef.Range.atLeast(1),
                        Importance.LOW,
                        KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DOC,
                        errorAndRetriesGroupName,
                        errorAndRetriesGroupOrder,
                        Width.MEDIUM,
                        KUSTO_SINK_RETRY_BACKOFF_TIME_MS_DISPLAY);
    }

    private static void defineWriteConfigs(@NotNull ConfigDef result, String tempDirectory) {
        final String writeGroupName = "Writes";
        int writeGroupOrder = 0;

        result
                .define(
                        KUSTO_TABLES_MAPPING_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        KUSTO_TABLES_MAPPING_DOC,
                        writeGroupName,
                        writeGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_TABLES_MAPPING_DISPLAY)
                .define(
                        KUSTO_SINK_TEMP_DIR_CONF,
                        Type.STRING,
                        tempDirectory,
                        Importance.LOW,
                        KUSTO_SINK_TEMP_DIR_DOC,
                        writeGroupName,
                        writeGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_SINK_TEMP_DIR_DISPLAY)
                .define(
                        KUSTO_SINK_FLUSH_SIZE_BYTES_CONF,
                        Type.LONG,
                        FileUtils.ONE_MB,
                        ConfigDef.Range.atLeast(100),
                        Importance.MEDIUM,
                        KUSTO_SINK_FLUSH_SIZE_BYTES_DOC,
                        writeGroupName,
                        writeGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_SINK_FLUSH_SIZE_BYTES_DISPLAY)
                .define(
                        KUSTO_SINK_FLUSH_INTERVAL_MS_CONF,
                        Type.LONG,
                        TimeUnit.SECONDS.toMillis(30),
                        ConfigDef.Range.atLeast(100),
                        Importance.HIGH,
                        KUSTO_SINK_FLUSH_INTERVAL_MS_DOC,
                        writeGroupName,
                        writeGroupOrder,
                        Width.MEDIUM,
                        KUSTO_SINK_FLUSH_INTERVAL_MS_DISPLAY)
                .define(
                        HEADERS_TO_PROJECT,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        HEADERS_TO_PROJECT_DOC,
                        writeGroupName,
                        writeGroupOrder++,
                        Width.MEDIUM,
                        HEADERS_TO_PROJECT_DISPLAY)
                .define(
                        HEADERS_TO_DROP,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        HEADERS_TO_DROP_DOC,
                        writeGroupName,
                        writeGroupOrder,
                        Width.MEDIUM,
                        HEADERS_TO_DROP_DISPLAY);
    }

    private static void defineConnectionConfigs(@NotNull ConfigDef result) {
        final String connectionGroupName = "Connection";
        int connectionGroupOrder = 0;
        result
                .define(
                        CONNECTION_STRING,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        CONNECTION_STRING_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        CONNECTION_STRING_DISPLAY)
                .define(
                        KUSTO_INGEST_URL_CONF,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        KUSTO_INGEST_URL_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_INGEST_URL_DISPLAY)
                .define(
                        KUSTO_ENGINE_URL_CONF,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        KUSTO_ENGINE_URL_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_ENGINE_URL_DISPLAY)
                .define(
                        KUSTO_AUTH_APPKEY_CONF,
                        Type.PASSWORD,
                        null,
                        Importance.HIGH,
                        KUSTO_AUTH_APPKEY_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_APPKEY_DISPLAY)
                .define(
                        KUSTO_AUTH_ACCESS_TOKEN_CONF,
                        Type.PASSWORD,
                        null,
                        Importance.LOW,
                        KUSTO_AUTH_ACCESS_TOKEN_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_ACCESS_TOKEN_DISPLAY)
                .define(
                        KUSTO_AUTH_APPID_CONF,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        KUSTO_AUTH_APPID_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_APPID_DISPLAY)
                .define(
                        KUSTO_AUTH_AUTHORITY_CONF,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        KUSTO_AUTH_AUTHORITY_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_AUTHORITY_DISPLAY)
                .define(
                        KUSTO_SINK_ENABLE_TABLE_VALIDATION,
                        Type.BOOLEAN,
                        Boolean.FALSE,
                        Importance.LOW,
                        KUSTO_SINK_ENABLE_TABLE_VALIDATION_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.SHORT,
                        KUSTO_SINK_ENABLE_TABLE_VALIDATION_DISPLAY)
                .define(
                        KUSTO_AUTH_STRATEGY_CONF,
                        Type.STRING,
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
                        Importance.HIGH,
                        KUSTO_AUTH_STRATEGY_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_AUTH_STRATEGY_DISPLAY)
                .define(
                        KUSTO_CONNECTION_PROXY_HOST,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        KUSTO_CONNECTION_PROXY_HOST_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.MEDIUM,
                        KUSTO_CONNECTION_PROXY_HOST_DISPLAY)
                .define(
                        KUSTO_CONNECTION_PROXY_PORT,
                        Type.INT,
                        -1,
                        Importance.LOW,
                        KUSTO_CONNECTION_PROXY_PORT_DOC,
                        connectionGroupName,
                        connectionGroupOrder,
                        Width.MEDIUM,
                        KUSTO_CONNECTION_PROXY_PORT_DISPLAY);
    }

    public String getConnectionString() {
        return this.getString(CONNECTION_STRING);
    }

    public FabricTarget getFabricTarget() {
        if (StringUtils.isNotEmpty(getConnectionString())) {
            if (getConnectionString().startsWith("sb://")) {
                return FabricTarget.EVENTSTREAM;
            }
            return FabricTarget.EVENTHOUSE;
        }
        if (StringUtils.isNotEmpty(getKustoIngestUrl())) {
            return FabricTarget.EVENTHOUSE;
        }
        throw new ConfigException("Either Kusto Ingestion URL or Connection String must be provided.");
    }

    public String getKustoIngestUrl() {
        String ingestionUrl = this.getString(KUSTO_INGEST_URL_CONF);
        if (StringUtils.isNotEmpty(ingestionUrl)) {
            return ingestionUrl;
        }
        return getUrlFromConnectionString(false);
    }

    private String getUrlFromConnectionString(boolean isEngineUrl) {
        String connectionString = this.getConnectionString();
        if (StringUtils.isEmpty(connectionString)) {
            throw new ConfigException("Either Kusto Ingestion URL or Connection String must be provided.");
        } else {
            ConnectionStringBuilder kustoConnectionStringBuilder = new ConnectionStringBuilder(connectionString);
            String engineUrl = kustoConnectionStringBuilder.getClusterUrl();
            if (isEngineUrl) {
                return engineUrl;
            } else {
                return engineUrl.replace("https://", "https://ingest-");
            }
        }
    }

    public void validateConfig() {
        if (getFabricTarget() == FabricTarget.EVENTHOUSE
                && StringUtils.isEmpty(getKustoIngestUrl())
                && StringUtils.isEmpty(getConnectionString())) {
            throw new ConfigException("One of kusto.ingestion.url or connection.string must be provided.");
        }
    }

    public String getKustoEngineUrl() {
        String clusterUrl = this.getString(KUSTO_ENGINE_URL_CONF);
        if (StringUtils.isNotEmpty(clusterUrl)) {
            return clusterUrl;
        }
        return getUrlFromConnectionString(true);
    }

    public String getAuthAppId() {
        return this.getString(KUSTO_AUTH_APPID_CONF);
    }

    public String getAuthAppKey() {
        return this.getPassword(KUSTO_AUTH_APPKEY_CONF).value();
    }

    public String getAuthAccessToken() {
        return this.getPassword(KUSTO_AUTH_ACCESS_TOKEN_CONF).value();
    }

    public String getAuthAuthority() {
        return this.getString(KUSTO_AUTH_AUTHORITY_CONF);
    }

    public KustoAuthenticationStrategy getAuthStrategy() {
        return KustoAuthenticationStrategy.valueOf(getString(KUSTO_AUTH_STRATEGY_CONF).toUpperCase(Locale.ENGLISH));
    }

    public String getRawTopicToTableMapping() {
        return getString(KUSTO_TABLES_MAPPING_CONF);
    }

    public TopicToTableMapping[] getTopicToTableMapping() throws JsonProcessingException {
        if (topicToTableMappingProperties.isEmpty()) {
            TopicToTableMapping[] mappings = OBJECT_MAPPER.readValue(getRawTopicToTableMapping(), TopicToTableMapping[].class);
            for (TopicToTableMapping mapping : mappings) {
                mapping.validate();
                topicToTableMappingProperties.put(mapping.getTopic(), mapping);
            }
        }
        return topicToTableMappingProperties.values().toArray(new TopicToTableMapping[0]);
    }

    // This is a redundant parser. However it is not a huge penalty that is incurred here on parse once or couple of times
    // Not making this synchronized in purpose
    public TopicToTableMapping getTopicToTableMapping(String topic) {
        return topicToTableMappingProperties.get(topic);
    }

    public HeaderTransforms headerTransforms() throws JsonProcessingException {
        CollectionType resultType = TypeFactory.defaultInstance().constructCollectionType(Set.class, String.class);
        String headersToProjectStr = getString(HEADERS_TO_PROJECT);
        String headersToDropStr = getString(HEADERS_TO_DROP);
        Set<String> headersToProject = StringUtils.isNotEmpty(headersToProjectStr) ? OBJECT_MAPPER.readValue(headersToProjectStr, resultType)
                : Collections.emptySet();
        Set<String> headersToDrop = StringUtils.isNotEmpty(headersToDropStr) ? OBJECT_MAPPER.readValue(headersToDropStr, resultType) : Collections.emptySet();
        return new HeaderTransforms(headersToDrop, headersToProject);
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
        props.put("bootstrap.servers", getDlqBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return props;
    }

    public int getMaxRetryAttempts() {
        return (int) (this.getLong(KUSTO_SINK_MAX_RETRY_TIME_MS_CONF)
                / this.getLong(KUSTO_SINK_RETRY_BACKOFF_TIME_MS_CONF));
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
        APPLICATION, MANAGED_IDENTITY, AZ_DEV_TOKEN, WORKLOAD_IDENTITY
    }

    public enum FabricTarget {
        EVENTHOUSE, EVENTSTREAM
    }
}
