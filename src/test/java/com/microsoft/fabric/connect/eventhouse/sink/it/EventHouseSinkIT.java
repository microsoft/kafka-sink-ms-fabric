package com.microsoft.fabric.connect.eventhouse.sink.it;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.storage.StringConverter;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.fabric.connect.eventhouse.sink.Utils;
import com.microsoft.fabric.connect.eventhouse.sink.Version;
import com.microsoft.fabric.connect.eventhouse.sink.it.containers.EventHouseKafkaConnectContainer;
import com.microsoft.fabric.connect.eventhouse.sink.it.containers.ProxyContainer;
import com.microsoft.fabric.connect.eventhouse.sink.it.containers.SchemaRegistryContainer;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import static com.microsoft.fabric.connect.eventhouse.sink.it.ITSetup.getConnectorProperties;
import static java.time.temporal.ChronoUnit.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

class EventHouseSinkIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHouseSinkIT.class);
    private static final Network network = Network.newNetwork();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Integer KAFKA_MAX_MSG_SIZE = 3 * 1024 * 1024;
    private static final String CONFLUENT_VERSION = "7.5.6";
    private static final String KAFKA_LISTENER = "kafka:19092";
    private static final ConfluentKafkaContainer kafkaContainer = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
                    .withListener(KAFKA_LISTENER).withReuse(true).withNetwork(network)
                    .withEnv("KAFKA_MESSAGE_MAX_BYTES", KAFKA_MAX_MSG_SIZE.toString())
                    .withEnv("KAFKA_SOCKET_REQUEST_MAX_BYTES", KAFKA_MAX_MSG_SIZE.toString());
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(CONFLUENT_VERSION, KAFKA_LISTENER).withNetwork(network)
            .dependsOn(kafkaContainer);
    private static final ProxyContainer proxyContainer = new ProxyContainer().withNetwork(network);
    private static final EventHouseKafkaConnectContainer connectContainer = new EventHouseKafkaConnectContainer(CONFLUENT_VERSION, KAFKA_LISTENER)
            .withKafka(kafkaContainer)
            .withNetwork(network).dependsOn(kafkaContainer, proxyContainer, schemaRegistryContainer);
    private static final String KEY_COLUMN = "vlong";
    private static final String COMPLEX_AVRO_BYTES_TABLE_TEST = String.format("ComplexAvroBytesTest_%s", UUID.randomUUID()).replace('-', '_');
    private static ITCoordinates coordinates;
    private static Client engineClient = null;
    private static Client dmClient = null;

    @BeforeAll
    public static void startContainers() throws Exception {
        coordinates = getConnectorProperties();
        if (coordinates.isValidConfig()) {
            ConnectionStringBuilder engineCsb = ConnectionStringBuilder
                    .createWithAadAccessTokenAuthentication(coordinates.cluster, coordinates.accessToken);
            ConnectionStringBuilder dmCsb = ConnectionStringBuilder
                    .createWithAadAccessTokenAuthentication(coordinates.ingestCluster, coordinates.accessToken);
            engineClient = ClientFactory.createClient(engineCsb);
            dmClient = ClientFactory.createClient(dmCsb);
            LOGGER.info("Creating tables in EventHouse");
            createTables();
            refreshDm();
            // Mount the libs
            String mountPath = String.format(
                    "target/kafka-sink-ms-fabric-%s-jar-with-dependencies.jar", Version.getConnectorVersion());
            LOGGER.info("Creating connector jar with version {} and mounting it from {}", Version.getConnectorVersion(), mountPath);
            Transferable source = MountableFile.forHostPath(mountPath);
            connectContainer.withCopyToContainer(source, Utils.getConnectPath());
            Startables.deepStart(Stream.of(kafkaContainer, schemaRegistryContainer, proxyContainer, connectContainer)).join();
            LOGGER.info("Started containers , copying scripts to container and executing them");
            connectContainer.withCopyToContainer(MountableFile.forClasspathResource("download-libs.sh", 744), // rwx--r--r--
                    String.format("%s/download-libs.sh", Utils.getConnectPath()))
                    .execInContainer("sh", String.format("%s/download-libs.sh", Utils.getConnectPath()));
            // Logs of start up of the container gets published here. This will be handy in case we want to look at startup failures
            LOGGER.debug(connectContainer.getLogs());
        } else {
            LOGGER.info("Skipping test due to missing configuration");
        }
    }

    private static void createTables() throws Exception {
        URL kqlResource = EventHouseSinkIT.class.getClassLoader().getResource("it-table-setup.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream()
                .map(kql -> kql.replace("TBL", coordinates.table))
                .map(kql -> kql.replace("CABT", COMPLEX_AVRO_BYTES_TABLE_TEST))
                .collect(Collectors.toList());
        kqlsToExecute.forEach(kql -> {
            try {
                engineClient.executeMgmt(coordinates.database, kql);
            } catch (Exception e) {
                LOGGER.error("Failed to execute kql: {}", kql, e);
            }
        });
        LOGGER.info("Created tables {} , {} and associated mappings", coordinates.table, COMPLEX_AVRO_BYTES_TABLE_TEST);
    }

    private static void refreshDm() throws Exception {
        URL kqlResource = EventHouseSinkIT.class.getClassLoader().getResource("dm-refresh-cache.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI()))
                .stream().map(kql -> kql.replace("TBL", coordinates.table))
                .map(kql -> kql.replace("DB", coordinates.database))
                .collect(Collectors.toList());
        kqlsToExecute.forEach(kql -> {
            try {
                dmClient.executeMgmt(kql);
            } catch (Exception e) {
                LOGGER.error("Failed to execute DM kql: {}", kql, e);
            }
        });
        LOGGER.info("Refreshed cache on DB {}", coordinates.database);
    }

    @AfterAll
    public static void stopContainers() {
        engineClient.executeMgmt(coordinates.database, String.format(".drop table %s", coordinates.table));
        engineClient.executeMgmt(coordinates.database, String.format(".drop table %s", COMPLEX_AVRO_BYTES_TABLE_TEST));
        engineClient.executeMgmt(coordinates.database, String.format(".drop table %s_d", coordinates.table));
        LOGGER.info("Finished table clean up. Dropped tables {} and {}", coordinates.table, COMPLEX_AVRO_BYTES_TABLE_TEST);
        connectContainer.stop();
        schemaRegistryContainer.stop();
        kafkaContainer.stop();
    }

    private static void deployConnector(@NotNull String dataFormat, String topicTableMapping,
            String srUrl, String keyFormat, String valueFormat) {
        deployConnector(dataFormat, topicTableMapping, srUrl, keyFormat, valueFormat, Collections.emptyMap());
    }

    private static void deployConnector(@NotNull String dataFormat, String topicTableMapping,
            String srUrl, String keyFormat, String valueFormat,
            Map<String, Object> overrideProps) {
        Map<String, Object> connectorProps = new HashMap<>();
        connectorProps.put("connector.class", "com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConnector");
        connectorProps.put("flush.size.bytes", 10000);
        connectorProps.put("flush.interval.ms", 1000);
        connectorProps.put("tasks.max", 1);
        connectorProps.put("topics", String.format("e2e.%s.topic", dataFormat));
        connectorProps.put("kusto.tables.topics.mapping", topicTableMapping);
        connectorProps.put("aad.auth.authority", coordinates.authority);
        connectorProps.put("aad.auth.accesstoken", coordinates.accessToken);
        connectorProps.put("aad.auth.strategy", "AZ_DEV_TOKEN".toLowerCase());
        connectorProps.put("kusto.query.url", coordinates.cluster);
        connectorProps.put("kusto.ingestion.url", coordinates.ingestCluster);
        if (!dataFormat.startsWith("bytes")) {
            connectorProps.put("schema.registry.url", srUrl);
            connectorProps.put("value.converter.schema.registry.url", srUrl);
        }
        connectorProps.put("key.converter", keyFormat);
        connectorProps.put("value.converter", valueFormat);
        connectorProps.put("proxy.host", proxyContainer.getContainerId().substring(0, 12));
        connectorProps.put("proxy.port", proxyContainer.getExposedPorts().get(0));
        connectorProps.putAll(overrideProps);
        String connectorName = overrideProps.getOrDefault("connector.name",
                String.format("adx-connector-%s", dataFormat)).toString();
        connectContainer.registerConnector(connectorName, connectorProps);
        LOGGER.debug("Deployed connector for {}", dataFormat);
        LOGGER.debug(connectContainer.getLogs());
        connectContainer.waitUntilConnectorTaskStateChanges(connectorName, 0, "RUNNING");
        LOGGER.info("Connector state for {} : {}. ", dataFormat,
                connectContainer.getConnectorTaskState(connectorName, 0));
    }

    @Execution(ExecutionMode.CONCURRENT)
    @ParameterizedTest(name = "Test for data format {0}")
    @CsvSource({"json", "avro", "csv", "bytes-json"})
    void shouldHandleAllTypesOfEvents(@NotNull String dataFormat) {
        LOGGER.info("Running test for data format {}", dataFormat);
        Assumptions.assumeTrue(coordinates.isValidConfig(), "Skipping test due to missing configuration");
        String srUrl = String.format("http://%s:%s", schemaRegistryContainer.getContainerId().substring(0, 12), 8081);
        String valueFormat = "org.apache.kafka.connect.storage.StringConverter";
        String keyFormat = "org.apache.kafka.connect.storage.StringConverter";
        if (dataFormat.equals("avro")) {
            valueFormat = AvroConverter.class.getName();
            LOGGER.debug("Using value format: {}", valueFormat);
        }
        // There are tests for CSV Queued. The other formats test for Streaming ingestion
        String topicTableMapping = dataFormat.equals("csv")
                ? String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s','mapping':'csv_mapping'}]",
                        dataFormat, coordinates.database, coordinates.table, dataFormat)
                : String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s','mapping':'data_mapping','streaming':'true'}]", dataFormat,
                        coordinates.database,
                        coordinates.table, dataFormat);
        if (dataFormat.startsWith("bytes")) {
            valueFormat = "org.apache.kafka.connect.converters.ByteArrayConverter";
            // JSON is written as JSON
            topicTableMapping = String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s'," +
                    "'mapping':'data_mapping'}]", dataFormat,
                    coordinates.database,
                    coordinates.table, dataFormat.split("-")[1]);
        }
        LOGGER.info("Deploying connector for {} , using SR url {}. Using proxy host {} and port {}", dataFormat, srUrl,
                proxyContainer.getContainerId().substring(0, 12), proxyContainer.getExposedPorts().get(0));
        deployConnector(dataFormat, topicTableMapping, srUrl, keyFormat, valueFormat);
        try {
            int maxRecords = 10;
            Map<Long, String> expectedRecordsProduced = produceKafkaMessages(dataFormat, maxRecords, "");
            if (expectedRecordsProduced.isEmpty()) {
                performTombstoneAssertions();
            } else {
                performDataAssertions(dataFormat, maxRecords, expectedRecordsProduced);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private @NotNull Map<Long, String> produceKafkaMessages(@NotNull String dataFormat, int maxRecords, String targetTopicName) throws IOException {
        LOGGER.warn("Producing messages");
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        // avro
        Generator.Builder builder = new Generator.Builder().schemaString(IOUtils.toString(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("it-avro.avsc")),
                StandardCharsets.UTF_8));
        Generator randomDataBuilder = builder.build();
        Map<Long, String> expectedRecordsProduced = new HashMap<>();
        String targetTopic = StringUtils.defaultIfBlank(targetTopicName, String.format("e2e.%s.topic", dataFormat));
        switch (dataFormat) {
            case "avro":
                producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                producerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        String.format("http://%s:%s", schemaRegistryContainer.getHost(), schemaRegistryContainer.getFirstMappedPort()));
                producerProperties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
                // GenericRecords to bytes using avro
                try (KafkaProducer<String, GenericData.Record> producer = new KafkaProducer<>(producerProperties)) {
                    for (int i = 0; i < maxRecords; i++) {
                        GenericData.Record genericRecord = (GenericData.Record) randomDataBuilder.generate();
                        genericRecord.put("vtype", dataFormat);
                        List<Header> headers = new ArrayList<>();
                        headers.add(new RecordHeader("Iteration", (dataFormat + "-Header" + i).getBytes()));
                        ProducerRecord<String, GenericData.Record> producerRecord = new ProducerRecord<>(targetTopic, 0, "Key-" + i, genericRecord, headers);
                        Map<String, Object> jsonRecordMap = genericRecord.getSchema().getFields().stream()
                                .collect(Collectors.toMap(Schema.Field::name, field -> genericRecord.get(field.name())));
                        jsonRecordMap.put("vtype", "avro");
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(KEY_COLUMN).toString()),
                                OBJECT_MAPPER.writeValueAsString(jsonRecordMap));
                        producer.send(producerRecord);
                    }
                }
                break;
            case "json":
                producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        String.format("http://%s:%s", schemaRegistryContainer.getHost(), schemaRegistryContainer.getFirstMappedPort()));
                producerProperties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
                // GenericRecords to json using avro
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
                    for (int i = 0; i < maxRecords; i++) {
                        GenericRecord genericRecord = (GenericRecord) randomDataBuilder.generate();
                        genericRecord.put("vtype", "json");
                        Map<String, Object> jsonRecordMap = genericRecord.getSchema().getFields().stream()
                                .collect(Collectors.toMap(Schema.Field::name, field -> genericRecord.get(field.name())));
                        List<Header> headers = new ArrayList<>();
                        headers.add(new RecordHeader("Iteration", (dataFormat + "-Header" + i).getBytes()));
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic,
                                0, "Key-" + i, OBJECT_MAPPER.writeValueAsString(jsonRecordMap), headers);
                        jsonRecordMap.put("vtype", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(KEY_COLUMN).toString()),
                                OBJECT_MAPPER.writeValueAsString(jsonRecordMap));
                        LOGGER.debug("JSON Record produced: {}", OBJECT_MAPPER.writeValueAsString(jsonRecordMap));
                        producer.send(producerRecord);
                        ProducerRecord<String, String> tombstoneRecord = new ProducerRecord<>(targetTopic,
                                0, "TSKey-" + i, null, headers);
                        producer.send(tombstoneRecord);
                    }
                }
                break;
            case "csv":
                producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        String.format("http://%s:%s", schemaRegistryContainer.getHost(), schemaRegistryContainer.getFirstMappedPort()));
                producerProperties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
                // GenericRecords to json using avro
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
                    for (int i = 0; i < maxRecords; i++) {
                        GenericRecord genericRecord = (GenericRecord) randomDataBuilder.generate();
                        genericRecord.put("vtype", "csv");
                        Map<String, Object> jsonRecordMap = new TreeMap<>(genericRecord.getSchema().getFields().stream().parallel()
                                .collect(Collectors.toMap(Schema.Field::name, field -> genericRecord.get(field.name()))));
                        LOGGER.info("JSON CSV Record produced: {}", jsonRecordMap);
                        String objectsCommaSeparated = jsonRecordMap.values().stream().map(Object::toString).collect(Collectors.joining(","));
                        LOGGER.info("CSV Record produced: {}", objectsCommaSeparated);
                        List<Header> headers = new ArrayList<>();
                        headers.add(new RecordHeader("Iteration", (dataFormat + "-Header" + i).getBytes()));
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic, 0, "Key-" + i,
                                objectsCommaSeparated, headers);
                        jsonRecordMap.put("vtype", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(KEY_COLUMN).toString()),
                                OBJECT_MAPPER.writeValueAsString(jsonRecordMap));
                        producer.send(producerRecord);
                    }
                }
                break;
            case "bytes-json":
                producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
                // GenericRecords to json using avro
                try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties)) {
                    for (int i = 0; i < maxRecords; i++) {
                        GenericRecord genericRecord = (GenericRecord) randomDataBuilder.generate();
                        genericRecord.put("vtype", "bytes-json");
                        // Serialization test for Avro as bytes , or JSON as bytes (Schemaless tests)
                        byte[] dataToSend = genericRecord.toString().getBytes(StandardCharsets.UTF_8);
                        Map<String, Object> jsonRecordMap = genericRecord.getSchema().getFields().stream()
                                .collect(Collectors.toMap(Schema.Field::name, field -> genericRecord.get(field.name())));
                        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(
                                targetTopic,
                                String.format("Key-%s", i),
                                dataToSend);
                        jsonRecordMap.put("vtype", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(KEY_COLUMN).toString()),
                                OBJECT_MAPPER.writeValueAsString(jsonRecordMap));
                        LOGGER.info("Bytes topic {} written to", targetTopic);
                        try {
                            RecordMetadata rmd = producer.send(producerRecord).get();
                            LOGGER.info("Record sent to topic {} with offset {} of size {}", targetTopic, rmd.offset(), dataToSend.length);
                        } catch (Exception e) {
                            LOGGER.error("Failed to send genericRecord to topic {}", String.format("e2e.%s.topic", dataFormat), e);
                        }
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown data format");
        }
        LOGGER.info("Produced messages for format {}", dataFormat);
        return expectedRecordsProduced;
    }

    private void performDataAssertions(@NotNull String dataFormat, int maxRecords, Map<Long, String> expectedRecordsProduced) {
        String query = String.format("%s | where vtype == '%s' | project  %s,vresult = pack_all()",
                coordinates.table, dataFormat, KEY_COLUMN);
        performDataAssertions(maxRecords, expectedRecordsProduced, query);
    }

    private void performDataAssertions(int maxRecords, Map<Long, String> expectedRecordsProduced, @NotNull String query) {
        Map<Object, String> actualRecordsIngested = getRecordsIngested(query, maxRecords);
        actualRecordsIngested.keySet().parallelStream().forEach(key -> {
            long keyLong = Long.parseLong(key.toString());
            LOGGER.debug("Record queried in assertion : {}", actualRecordsIngested.get(key));
            try {
                JSONAssert.assertEquals(expectedRecordsProduced.get(keyLong), actualRecordsIngested.get(key),
                        new CustomComparator(LENIENT,
                                // there are sometimes round off errors in the double values, but they are close enough to 8 precision
                                new Customization("vdec", (vdec1,
                                        vdec2) -> Math.abs(Double.parseDouble(vdec1.toString()) - Double.parseDouble(vdec2.toString())) < 0.000000001),
                                new Customization("vreal", (vreal1,
                                        vreal2) -> Math.abs(Double.parseDouble(vreal1.toString()) - Double.parseDouble(vreal2.toString())) < 0.0001)));
            } catch (JSONException e) {
                fail(e);
            }
        });
    }

    private static void performTombstoneAssertions() {
        try {
            String tsQuery = String.format("%s | where keys startswith 'TSKey'|project vresult = pack_all()", coordinates.table);
            KustoResultSetTable tsResultSet = engineClient.executeQuery(coordinates.database, tsQuery).getPrimaryResults();
            TypeReference<Map<String, Object>> mapResultRef = new TypeReference<Map<String, Object>>() {};
            while (tsResultSet.next()) {
                String nullRecords = tsResultSet.getString("vresult");
                Map<String, Object> parsedResult = OBJECT_MAPPER.readValue(nullRecords, mapResultRef);
                parsedResult.forEach((key, value) -> {
                    if (key.startsWith("v")) {
                        assertTrue(StringUtils.isEmpty(value.toString()), "Field " + key + " " +
                                "should be null/empty but is " + value);
                    } else {
                        assertNotNull(value, "Field " + key + " is null");
                    }
                });
            }
        } catch (Exception e) {
            fail(e);
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @Test
    void shouldHandleComplexAvroMessage() throws IOException {
        String dataFormat = "bytes-avro";
        int maxRecords = 8;
        String srUrl = String.format("http://%s:%s", schemaRegistryContainer.getContainerId().substring(0, 12), 8081);
        String producerSrUrl = String.format("http://localhost:%s", schemaRegistryContainer.getMappedPort(8081));
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, producerSrUrl);
        producerProperties.put("key.schema.registry.url", producerSrUrl);
        producerProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, KAFKA_MAX_MSG_SIZE);
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, KAFKA_MAX_MSG_SIZE * 5);
        producerProperties.put("message.max.bytes", KAFKA_MAX_MSG_SIZE);
        String topicName = String.format("e2e.%s.topic", dataFormat);
        String topicTableMapping = String.format("[{'topic': '%s','db': '%s', " +
                "'table': '%s','format':'%s','mapping':'%s_mapping'}]", topicName,
                coordinates.database,
                COMPLEX_AVRO_BYTES_TABLE_TEST, dataFormat.split("-")[1], COMPLEX_AVRO_BYTES_TABLE_TEST);
        deployConnector(dataFormat, topicTableMapping, srUrl,
                AvroConverter.class.getName(),
                "org.apache.kafka.connect.converters.ByteArrayConverter",
                Collections.singletonMap("key.converter.schema.registry.url", srUrl));
        Schema keySchema = SchemaBuilder
                .record("Key").namespace("com.ms.kafka.connect.sink.avro")
                .fields()
                .name("IterationKey").type().stringType().noDefault()
                .name("Timestamp").type().nullable().longType().noDefault()
                .endRecord();
        long keyStart = 100000L;
        InputStream expectedResultsStream = Objects
                .requireNonNull(this.getClass().getClassLoader().getResourceAsStream("avro-complex-data/expected-results.txt"));
        String expectedResults = IOUtils.toString(expectedResultsStream, StandardCharsets.UTF_8);
        Map<String, String> expectedResultMap = Arrays.stream(expectedResults.split("\n"))
                .map(line -> line.split("~"))
                .collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
        try (KafkaProducer<GenericData.Record, byte[]> producer = new KafkaProducer<>(producerProperties)) {
            for (int i = 1; i <= maxRecords; i++) {
                // complex-avro-1.avro
                long keyTick = keyStart + i;
                GenericData.Record keyRecord = new GenericData.Record(keySchema);
                keyRecord.put("IterationKey", String.valueOf(i));
                keyRecord.put("Timestamp", keyTick);
                InputStream avroData = Objects
                        .requireNonNull(this.getClass().getClassLoader().getResourceAsStream(
                                String.format("avro-complex-data/complex-avro-%d.avro", i)));
                byte[] testData = IOUtils.toByteArray(avroData);
                ProducerRecord<GenericData.Record, byte[]> producerRecord = new ProducerRecord<>(topicName, keyRecord, testData);
                producerRecord.headers().add("vtype", dataFormat.getBytes());
                producerRecord.headers().add("iteration", String.valueOf(i).getBytes());
                RecordMetadata rmd = producer.send(producerRecord).get();
                LOGGER.info("Avro bytes sent to topic {} with offset {} of size {}", topicName, rmd.offset(), testData.length);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to send record to topic {}", topicName, e);
            fail("Failed sending message to Kafka for testing Avro-Bytes scenario.");
        }
        String countLongQuery = String.format("%s | summarize c = count() by event_id | project %s=event_id, " +
                "vresult = bag_pack('event_id',event_id,'count',c)", COMPLEX_AVRO_BYTES_TABLE_TEST, KEY_COLUMN);
        Map<Object, String> actualRecordsIngested = getRecordsIngested(countLongQuery, maxRecords);
        Awaitility.await().atMost(Duration.of(1, MINUTES)).untilAsserted(() -> assertEquals(maxRecords, actualRecordsIngested.size()));
        assertEquals(expectedResultMap, actualRecordsIngested);
    }

    /**
     * Polls the EventHouse table for the records ingested. The query is executed every 30 seconds and the results are
     * @param query      The query to execute
     * @param maxRecords The maximum number of records to poll for
     * @return A map of the records ingested
     */
    private @NotNull Map<Object, String> getRecordsIngested(String query, int maxRecords) {
        Predicate<Object> predicate = results -> {
            if (results != null && !((Map<?, ?>) results).isEmpty()) {
                LOGGER.info("Retrieved records count {}", ((Map<?, ?>) results).size());
            }
            return results == null || ((Map<?, ?>) results).isEmpty() || ((Map<?, ?>) results).size() < maxRecords;
        };
        // Waits 30 seconds for the records to be ingested. Repeats the poll 5 times , in all 150 seconds
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(5)
                .retryOnResult(predicate)
                .waitDuration(Duration.of(30, SECONDS))
                .build();
        RetryRegistry registry = RetryRegistry.of(config);
        Retry retry = registry.retry("ingestRecordService", config);
        Supplier<Map<Object, String>> recordSearchSupplier = () -> {
            try {
                LOGGER.debug("Executing query {} ", query);
                KustoResultSetTable resultSet = engineClient.executeQuery(coordinates.database, query).getPrimaryResults();
                Map<Object, String> actualResults = new HashMap<>();
                while (resultSet.next()) {
                    Object keyObject = resultSet.getObject(KEY_COLUMN);
                    Object key = keyObject instanceof Number ? Long.parseLong(keyObject.toString()) : keyObject.toString();
                    String vResult = resultSet.getString("vresult");
                    LOGGER.debug("Record queried from DB: {}", vResult);
                    actualResults.put(key, vResult);
                }
                return actualResults;
            } catch (DataServiceException | DataClientException e) {
                return Collections.emptyMap();
            }
        };
        return retry.executeSupplier(recordSearchSupplier);
    }

    @Execution(ExecutionMode.CONCURRENT)
    @ParameterizedTest(name = "Test DLQ tests for data format {0}")
    @CsvSource({"avro", "json"})
    void testWritesToDlq(String dataFormat) throws IOException {
        // The goal is to check writes to DLQ for different message formats and not really how it is triggered which
        // are mostly runtime faults
        String srUrl = String.format("http://%s:%s", schemaRegistryContainer.getContainerId().substring(0, 12), 8081);
        String topicTableMapping = String.format("[{'topic': 'e2e.%s-err.topic','db': '%s', 'table': '%s','format':'%s'," +
                "'mapping':'data_mapping'}]", dataFormat,
                coordinates.database,
                coordinates.table, dataFormat);
        String valueFormat = StringConverter.class.getName();
        String keyFormat = StringConverter.class.getName();
        if (dataFormat.equals("avro")) {
            keyFormat = StringConverter.class.getName();
            valueFormat = AvroConverter.class.getName();
        }
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("behavior.on.error", "log");
        overrides.put("misc.deadletterqueue.bootstrap.servers", kafkaContainer.getEnvMap().get("KAFKA_LISTENERS"));
        String dlqTopicName = String.format("e2e.tests.%s.dlq.topic", dataFormat);
        overrides.put("misc.deadletterqueue.topic.name", dlqTopicName);
        overrides.put("proxy.host", proxyContainer.getContainerId().substring(0, 12));
        overrides.put("proxy.port", proxyContainer.getExposedPorts().get(0));
        overrides.put("connector.name", String.format("dlq-connector-%s", dataFormat));
        overrides.put("schema.registry.url", srUrl);
        overrides.put("value.converter.schema.registry.url", srUrl);
        overrides.put("key.converter.schema.registry.url", srUrl);
        String targetTopic = String.format("e2e.%s-err.topic", dataFormat);
        overrides.put("topics", targetTopic);
        overrides.put("kusto.ingestion.url", coordinates.ingestCluster + ".xxx");
        deployConnector(dataFormat, topicTableMapping, srUrl,
                keyFormat,
                valueFormat,
                overrides);
        produceKafkaMessages(dataFormat, 100, targetTopic);
        Collection<ConsumerRecord<byte[], byte[]>> dlqConsumerRecords = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(getProperties(dataFormat))) {
            consumer.subscribe(Collections.singletonList(dlqTopicName));
            Awaitility.await().atMost(Duration.of(2, MINUTES)).until(() -> {
                ConsumerRecords<byte[], byte[]> records = pollMessages(consumer);
                records.forEach(dlqConsumerRecords::add);
                return dlqConsumerRecords.size() >= 100;
            });
        }
        dlqConsumerRecords.forEach(consumerRecord -> {
            assertNotNull(consumerRecord.headers().lastHeader("kafka_offset"));
            assertNotNull(consumerRecord.headers().lastHeader("kafka_partition"));
            assertNotNull(consumerRecord.headers().lastHeader("kafka_topic"));
        });
    }

    @Execution(ExecutionMode.CONCURRENT)
    @ParameterizedTest(name = "Test dynamic generic payload for data format {0}")
    @CsvSource({"avro", "json"})
    void testDynamicPayloads(String dataFormat) throws Exception {
        String targetTopic = String.format("e2e.%s-dynamic.topic", dataFormat);
        // The goal is to check writes to DLQ for different message formats and not really how it is triggered which
        // are mostly runtime faults
        String srUrl = String.format("http://%s:%s", schemaRegistryContainer.getContainerId().substring(0, 12), 8081);
        String topicTableMapping = String.format("[{'topic': '%s','db': '%s', 'table': '%s_d','format':'%s'," +
                "'dynamicPayload':'true'}]", targetTopic,
                coordinates.database,
                coordinates.table, dataFormat);
        String valueFormat = StringConverter.class.getName();
        String keyFormat = StringConverter.class.getName();
        if (dataFormat.equals("avro")) {
            valueFormat = AvroConverter.class.getName();
        }
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("behavior.on.error", "log");
        overrides.put("connector.name", String.format("dynamic-connector-%s", dataFormat));
        overrides.put("topics", targetTopic);
        overrides.put("schema.registry.url", srUrl);
        overrides.put("value.converter.schema.registry.url", srUrl);
        overrides.put("key.converter.schema.registry.url", srUrl);
        deployConnector(dataFormat, topicTableMapping, srUrl,
                keyFormat,
                valueFormat,
                overrides);
        String query = String.format("%s_d | evaluate bag_unpack(payload) | where vtype == '%s' | project  %s,vresult = pack_all()",
                coordinates.table, dataFormat, KEY_COLUMN);
        int maxRecords = 10;
        Map<Long, String> expectedRecordsProduced = produceKafkaMessages(dataFormat, maxRecords, targetTopic);
        if (expectedRecordsProduced.isEmpty()) {
            performTombstoneAssertions();
        } else {
            performDataAssertions(maxRecords, expectedRecordsProduced, query);
        }

    }

    private static ConsumerRecords<byte[], byte[]> pollMessages(@NotNull KafkaConsumer<byte[], byte[]> consumer) {
        return consumer.poll(Duration.of(1000, MILLIS)); // Poll for up to 1000ms
    }

    private static @NotNull Properties getProperties(@NotNull String dataFormat) {
        LOGGER.info("Creating consumer properties for DLQ for format {}", dataFormat);
        String keyDeserializer = ByteArrayDeserializer.class.getName();
        String valueDeserializer = ByteArrayDeserializer.class.getName();
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        consumerProperties.put("group.id", "dlq-consumer");
        consumerProperties.put("key.deserializer", keyDeserializer);
        consumerProperties.put("value.deserializer", valueDeserializer);
        consumerProperties.put("auto.offset.reset", "earliest");
        return consumerProperties;
    }
}
