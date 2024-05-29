package com.microsoft.fabric.kafka.connect.sink.it;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.fabric.kafka.connect.sink.Version;
import com.microsoft.fabric.kafka.connect.sink.it.containers.KustoKafkaConnectContainer;
import com.microsoft.fabric.kafka.connect.sink.it.containers.ProxyContainer;
import com.microsoft.fabric.kafka.connect.sink.it.containers.SchemaRegistryContainer;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

class FabricSinkIT {
    // extract repeated strings
    private static final String ES_CONNECTOR = "es-connector-%s";
    private static final String KQLDB_CONNECTOR = "kqldb-connector-%s";
    private static final String E2E_TOPIC = "e2e.%s.topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(FabricSinkIT.class);
    private static final Network network = Network.newNetwork();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Integer KAFKA_MAX_MSG_SIZE = 3 * 1024 * 1024;
    private static final String confluentVersion = "6.2.5";
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + confluentVersion))
            .withNetwork(network)
            .withEnv("KAFKA_MESSAGE_MAX_BYTES", KAFKA_MAX_MSG_SIZE.toString())
            .withEnv("KAFKA_SOCKET_REQUEST_MAX_BYTES", KAFKA_MAX_MSG_SIZE.toString());
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(confluentVersion).withKafka(kafkaContainer)
            .withNetwork(network).dependsOn(kafkaContainer);
    private static final ProxyContainer proxyContainer = new ProxyContainer().withNetwork(network);
    private static final KustoKafkaConnectContainer connectContainer = new KustoKafkaConnectContainer(confluentVersion)
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .dependsOn(kafkaContainer, proxyContainer, schemaRegistryContainer);
    private static final String keyColumn = "vlong";
    private static final String COMPLEX_AVRO_BYTES_TABLE_TEST =
            String.format("ComplexAvroBytesTest_%s",UUID.randomUUID()).replace('-', '_');
    private static final String TBL_KEYWORD = "TBL";
    private static final String CABT_KEYWORD = "CABT";
    private static ITCoordinates coordinates;
    private static Client engineClient = null;
    private static Client dmClient = null;

    @BeforeAll
    @SuppressWarnings("deprecation")
    public static void startContainers() throws Exception {
        coordinates = ITSetup.getConnectorProperties();
        String engineUrl = new ConnectionStringBuilder(coordinates.kqlDbConnectionString).getClusterUrl();
        String ingestCluster = engineUrl.replaceAll("https://", "https://ingest-");
        if (coordinates.isValidConfig()) {
            ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(engineUrl,
                    coordinates.accessToken);
            ConnectionStringBuilder dmCsb = ConnectionStringBuilder.
                    createWithAadAccessTokenAuthentication(ingestCluster, coordinates.accessToken);
            engineClient = ClientFactory.createClient(engineCsb);
            dmClient = ClientFactory.createClient(dmCsb);
            LOGGER.info("Creating tables in Kusto");
            createTables();
            refreshDm();
            // Mount the libs
            String mountPath = String.format(
                    "target/components/packages/microsoftcorporation-kafka-sink-ms-fabric-%s/microsoftcorporation-kafka-sink-ms-fabric-%s/lib",
                    Version.getVersion(), Version.getVersion());
            LOGGER.info("Creating connector jar with version {} and mounting it from {},", Version.getVersion(), mountPath);
            connectContainer.withFileSystemBind(mountPath, "/kafka/connect/kafka-sink-ms-fabric");
            Startables.deepStart(Stream.of(kafkaContainer, schemaRegistryContainer, proxyContainer, connectContainer)).join();
            LOGGER.info("Started containers , copying scripts to container and executing them");
            connectContainer.withCopyToContainer(MountableFile.forClasspathResource("download-libs.sh", 744), // rwx--r--r--
                    "/kafka/connect/kafka-sink-ms-fabric/download-libs.sh").execInContainer("sh", "/kafka/connect/kafka-sink-ms-fabric/download-libs.sh");
            // Logs of start up of the container gets published here. This will be handy in case we want to look at startup failures
            LOGGER.debug(connectContainer.getLogs());
        } else {
            LOGGER.debug("Skipping test due to missing configuration");
        }
    }

    private static void createTables() throws Exception {
        URL kqlResource = FabricSinkIT.class.getClassLoader().getResource("it-table-setup.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream()
                .map(kql -> kql.replace(TBL_KEYWORD, coordinates.table))
                .map(kql -> kql.replace(CABT_KEYWORD,COMPLEX_AVRO_BYTES_TABLE_TEST))
                .collect(Collectors.toList());
        kqlsToExecute.forEach(kql -> {
            try {
                engineClient.execute(coordinates.database, kql);
            } catch (Exception e) {
                LOGGER.error("Failed to execute kql: {}", kql, e);
            }
        });
        LOGGER.info("Created tables {} , {} and associated mappings", coordinates.table , COMPLEX_AVRO_BYTES_TABLE_TEST);
    }

    private static void refreshDm() throws Exception {
        URL kqlResource = FabricSinkIT.class.getClassLoader().getResource("dm-refresh-cache.kql");
        assert kqlResource != null;
        List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream().map(kql -> kql.replace(TBL_KEYWORD, coordinates.table))
                .map(kql -> kql.replace("DB", coordinates.database))
                .collect(Collectors.toList());
        kqlsToExecute.forEach(kql -> {
            try {
                dmClient.execute(kql);
            } catch (Exception e) {
                LOGGER.error("Failed to execute DM kql: {}", kql, e);
            }
        });
        LOGGER.info("Refreshed cache on DB {}", coordinates.database);
    }

    @AfterAll
    public static void stopContainers() throws Exception {
        Thread.sleep(100_000);
        LOGGER.info("Finished table clean up. Dropped tables {} and {}", coordinates.table, COMPLEX_AVRO_BYTES_TABLE_TEST);
        connectContainer.stop();
        schemaRegistryContainer.stop();
        kafkaContainer.stop();
        engineClient.execute(coordinates.database, String.format(".drop table %s", coordinates.table));
        engineClient.execute(coordinates.database, String.format(".drop table %s", COMPLEX_AVRO_BYTES_TABLE_TEST));
        dmClient.close();
        engineClient.close();
    }

    private static void deployKqlDbConnector(@NotNull String dataFormat, String topicTableMapping,
                                             String srUrl, String keyFormat, String valueFormat) {
        deployKqlDbConnector(dataFormat, coordinates.kqlDbConnectionString, topicTableMapping, srUrl, keyFormat, valueFormat, Collections.emptyMap());
        // Only test EventStream case for AVRO and JSON
        if(dataFormat.equals("avro") || dataFormat.equals("json")) {
            deployEventStreamConnector(dataFormat, coordinates.esConnectionString,srUrl, keyFormat, valueFormat, Collections.emptyMap());
        }
    }

    private static void deployEventStreamConnector(@NotNull String dataFormat, String connectionString,
                                             String srUrl, String keyFormat, String valueFormat,
                                             Map<String, Object> overrideProps) {
        Map<String, Object> connectorProps = new HashMap<>();
        LOGGER.info("Using EventStream co-ordinates {} ",connectionString.split(";")[0]);
        connectorProps.put("connector.class", "com.microsoft.fabric.kafka.connect.sink.FabricSinkConnector");
        connectorProps.put("connection.string", connectionString);
        connectorProps.put("eventhub.name", coordinates.eventHub);
        connectorProps.put("client.name", String.format(ES_CONNECTOR, dataFormat));
        connectorProps.put("message.format", dataFormat);
        if (!dataFormat.startsWith("bytes")) {
            connectorProps.put("schema.registry.url", srUrl);
            connectorProps.put("value.converter.schema.registry.url", srUrl);
        }
        connectorProps.put("topics", String.format(E2E_TOPIC, dataFormat));
        connectorProps.put("key.converter", keyFormat);
        connectorProps.put("value.converter", valueFormat);
        connectorProps.put("tasks.max", 1);
        connectorProps.putAll(overrideProps);
        connectContainer.registerConnector(String.format(ES_CONNECTOR, dataFormat), connectorProps);
        LOGGER.debug("EventStream connector deployed for {}", dataFormat);
        LOGGER.debug(connectContainer.getLogs());
        connectContainer.waitUntilConnectorTaskStateChanges(String.format(ES_CONNECTOR, dataFormat), 0, "RUNNING");
        LOGGER.info("EventStream Connector state for {} : {}. ", dataFormat,
                connectContainer.getConnectorTaskState(String.format(ES_CONNECTOR, dataFormat), 0));
    }


    private static void deployKqlDbConnector(@NotNull String dataFormat, String connectionString, String topicTableMapping,
                                             String srUrl, String keyFormat, String valueFormat,
                                             Map<String, Object> overrideProps) {
        Map<String, Object> connectorProps = new HashMap<>();
        connectorProps.put("connector.class", "com.microsoft.fabric.kafka.connect.sink.FabricSinkConnector");
        connectorProps.put("flush.size.bytes", 10000);
        connectorProps.put("flush.interval.ms", 1000);
        connectorProps.put("tasks.max", 1);
        connectorProps.put("topics", String.format(E2E_TOPIC, dataFormat));
        connectorProps.put("kusto.tables.topics.mapping", topicTableMapping);
        connectorProps.put("aad.auth.accesstoken", coordinates.accessToken);
        connectorProps.put("aad.auth.strategy", "AZ_DEV_TOKEN".toLowerCase());
        connectorProps.put("connection.string", connectionString);
        if (!dataFormat.startsWith("bytes")) {
            connectorProps.put("schema.registry.url", srUrl);
            connectorProps.put("value.converter.schema.registry.url", srUrl);
        }
        connectorProps.put("key.converter", keyFormat);
        connectorProps.put("value.converter", valueFormat);
        connectorProps.put("proxy.host", proxyContainer.getContainerId().substring(0, 12));
        connectorProps.put("proxy.port", proxyContainer.getExposedPorts().get(0));
        connectorProps.putAll(overrideProps);
        connectContainer.registerConnector(String.format(KQLDB_CONNECTOR, dataFormat), connectorProps);
        LOGGER.debug("Deployed connector for {}", dataFormat);
        LOGGER.debug(connectContainer.getLogs());
        connectContainer.waitUntilConnectorTaskStateChanges(String.format(KQLDB_CONNECTOR, dataFormat), 0, "RUNNING");
        LOGGER.info("Connector state for {} : {}. ", dataFormat,
                connectContainer.getConnectorTaskState(String.format(KQLDB_CONNECTOR, dataFormat), 0));
    }

    @ParameterizedTest
    @CsvSource({"json", "avro"}) // , "csv" , "bytes-json"
    public void shouldHandleAllTypesOfEventsToEventHouse(@NotNull String dataFormat) {
        LOGGER.info("Running test for data format {}", dataFormat);
        Assumptions.assumeTrue(coordinates.isValidConfig(), "Skipping test due to missing configuration");
        String srUrl = String.format("http://%s:%s", schemaRegistryContainer.getContainerId().substring(0, 12), 8081);
        String valueFormat = "org.apache.kafka.connect.storage.StringConverter";
        String keyFormat = "org.apache.kafka.connect.storage.StringConverter";
        if (dataFormat.equals("avro")) {
            valueFormat = AvroConverter.class.getName();
            LOGGER.debug("Using value format: {}", valueFormat);
        }
        String topicTableMapping = dataFormat.equals("csv")
                ? String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s','mapping':'csv_mapping','streaming':'true'}]",
                dataFormat, coordinates.database, coordinates.table, dataFormat)
                : String.format("[{'topic': 'e2e.%s.topic','db': '%s', 'table': '%s','format':'%s','mapping':'data_mapping'}]", dataFormat,
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
        deployKqlDbConnector(dataFormat, topicTableMapping, srUrl, keyFormat, valueFormat);
        try {
            produceKafkaMessages(dataFormat);
            Thread.sleep(60_000);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void produceKafkaMessages(@NotNull String dataFormat) throws IOException {
        LOGGER.info("Producing messages");
        int maxRecords = 10;
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        // avro
        Generator.Builder builder = new Generator.Builder().schemaString(IOUtils.toString(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("it-avro.avsc")),
                StandardCharsets.UTF_8));
        Generator randomDataBuilder = builder.build();
        Map<Long, String> expectedRecordsProduced = new HashMap<>();

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
                        GenericData.Record record = (GenericData.Record) randomDataBuilder.generate();
                        record.put("vtype", dataFormat);
                        ProducerRecord<String, GenericData.Record> producerRecord =
                                new ProducerRecord<>("e2e.avro.topic", "Key-" + i, record);
                        Map<String, Object> jsonRecordMap = record.getSchema().getFields().stream()
                                .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name())));
                        jsonRecordMap.put("vtype", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(keyColumn).toString()),
                                objectMapper.writeValueAsString(jsonRecordMap));
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
                        GenericRecord record = (GenericRecord) randomDataBuilder.generate();
                        record.put("vtype", dataFormat);
                        Map<String, Object> jsonRecordMap = record.getSchema().getFields().stream()
                                .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name())));
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("e2e.json.topic", "Key-" + i,
                                objectMapper.writeValueAsString(jsonRecordMap));
                        jsonRecordMap.put("vtype", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(keyColumn).toString()),
                                objectMapper.writeValueAsString(jsonRecordMap));
                        LOGGER.debug("JSON Record produced: {}", objectMapper.writeValueAsString(jsonRecordMap));
                        producer.send(producerRecord);
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
                        GenericRecord record = (GenericRecord) randomDataBuilder.generate();
                        record.put("vtype", dataFormat);
                        Map<String, Object> jsonRecordMap = new TreeMap<>(record.getSchema().getFields().stream().parallel()
                                .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name()))));
                        String objectsCommaSeparated = jsonRecordMap.values().stream().map(Object::toString).collect(Collectors.joining(","));
                        LOGGER.debug("CSV Record produced: {}", objectsCommaSeparated);
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("e2e.csv.topic", "Key-" + i,
                                objectsCommaSeparated);
                        jsonRecordMap.put("vtype", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(keyColumn).toString()),
                                objectMapper.writeValueAsString(jsonRecordMap));
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
                        GenericRecord record = (GenericRecord) randomDataBuilder.generate();
                        record.put("vtype", dataFormat);
                        // Serialization test for Avro as bytes , or JSON as bytes (Schemaless tests)
                        byte[] dataToSend = record.toString().getBytes(StandardCharsets.UTF_8);
                        Map<String, Object> jsonRecordMap = record.getSchema().getFields().stream()
                                .collect(Collectors.toMap(Schema.Field::name, field -> record.get(field.name())));
                        ProducerRecord<String, byte[]> producerRecord =
                                new ProducerRecord<>(
                                        String.format(E2E_TOPIC, dataFormat),
                                        String.format("Key-%s", i),
                                        dataToSend);
                        jsonRecordMap.put("vtype", dataFormat);
                        expectedRecordsProduced.put(Long.valueOf(jsonRecordMap.get(keyColumn).toString()),
                                objectMapper.writeValueAsString(jsonRecordMap));
                        LOGGER.info("Bytes topic {} written to", String.format(E2E_TOPIC, dataFormat));
                        try {
                            RecordMetadata rmd = producer.send(producerRecord).get();
                            LOGGER.info("Record sent to topic {} with offset {} of size {}",
                                    String.format(E2E_TOPIC, dataFormat), rmd.offset(), dataToSend.length);
                        } catch (Exception e) {
                            LOGGER.error("Failed to send record to topic {}", String.format(E2E_TOPIC, dataFormat), e);
                        }
                    }
                }
                break;
        }
        LOGGER.info("Produced messages for format {}", dataFormat);
        String query = String.format("%s | where vtype == '%s' | project  %s,vresult = pack_all()",
                coordinates.table, dataFormat, keyColumn);
        Map<Object, String> actualRecordsIngested = getRecordsIngested(query, maxRecords);
        actualRecordsIngested.keySet().parallelStream().forEach(key -> {
            long keyLong = Long.parseLong(key.toString());
            LOGGER.debug("Actual record : {}", actualRecordsIngested.get(key));
            try {
                JSONAssert.assertEquals(expectedRecordsProduced.get(keyLong), actualRecordsIngested.get(key),
                        new CustomComparator(LENIENT,
                                // there are sometimes round off errors in the double values but they are close enough to 8 precision
                                new Customization("vdec", (vdec1,
                                                           vdec2) -> Math.abs(Double.parseDouble(vdec1.toString()) - Double.parseDouble(vdec2.toString())) < 0.000000001),
                                new Customization("vreal", (vreal1,
                                                            vreal2) -> Math.abs(Double.parseDouble(vreal1.toString()) - Double.parseDouble(vreal2.toString())) < 0.0001)));
            } catch (JSONException e) {
                fail(e);
            }
        });
        assertEquals(maxRecords, actualRecordsIngested.size());
    }

    @Disabled
    @Test
    public void shouldHandleComplexAvroMessage() throws IOException {
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
        String topicName = String.format(E2E_TOPIC, dataFormat);
        String topicTableMapping = String.format("[{'topic': '%s','db': '%s', " +
                        "'table': '%s','format':'%s','mapping':'%s_mapping'}]", topicName,
                coordinates.database,
                COMPLEX_AVRO_BYTES_TABLE_TEST, dataFormat.split("-")[1], COMPLEX_AVRO_BYTES_TABLE_TEST);
        deployKqlDbConnector(dataFormat, coordinates.kqlDbConnectionString,topicTableMapping, srUrl,
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

        InputStream expectedResultsStream = Objects.requireNonNull(this.getClass().getClassLoader().
                getResourceAsStream("avro-complex-data/expected-results.txt"));
        String expectedResults = IOUtils.toString(expectedResultsStream, StandardCharsets.UTF_8);
        Map<String, String> expectedResultMap =
                Arrays.stream(expectedResults.split("\n"))
                        .map(line -> line.split("~"))
                        .collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
        try (KafkaProducer<GenericData.Record, byte[]> producer = new KafkaProducer<>(producerProperties)) {
            for (int i = 1; i <= maxRecords; i++) {
                //complex-avro-1.avro
                long keyTick = keyStart + i;
                GenericData.Record keyRecord = new GenericData.Record(keySchema);
                keyRecord.put("IterationKey", String.valueOf(i));
                keyRecord.put("Timestamp", keyTick);
                InputStream avroData = Objects.requireNonNull(this.getClass().getClassLoader().
                        getResourceAsStream(String.format("avro-complex-data/complex-avro-%d.avro", i)));
                byte[] testData = IOUtils.toByteArray(avroData);
                ProducerRecord<GenericData.Record, byte[]> producerRecord = new ProducerRecord<>(topicName, keyRecord, testData);
                producerRecord.headers().add("vtype", dataFormat.getBytes());
                producerRecord.headers().add("iteration", String.valueOf(i).getBytes());
                RecordMetadata rmd = producer.send(producerRecord).get();
                LOGGER.info("Avro bytes sent to topic {} with offset {} of size {}", topicName, rmd.offset(), testData.length);
            }
            Thread.sleep(30_000);
        } catch (Exception e) {
            LOGGER.error("Failed to send record to topic {}", topicName, e);
            fail("Failed sending message to Kafka for testing Avro-Bytes scenario.");
        }

        String countLongQuery = String.format("%s | summarize c = count() by event_id | project %s=event_id, " +
                        "vresult = bag_pack('event_id',event_id,'count',c)", COMPLEX_AVRO_BYTES_TABLE_TEST, keyColumn);

        Map<Object, String> actualRecordsIngested = getRecordsIngested(countLongQuery, maxRecords);
        assertEquals(expectedResultMap, actualRecordsIngested);
    }

    /**
     * Polls the Kusto table for the records ingested. The query is executed every 30 seconds and the results are
     * @param query The query to execute
     * @param maxRecords The maximum number of records to poll for
     * @return A map of the records ingested
     */
    private @NotNull Map<Object, String> getRecordsIngested(String query , int maxRecords) {
        Predicate<Object> predicate = (results) -> {
            if (results != null) {
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
                KustoResultSetTable resultSet = engineClient.execute(coordinates.database, query).getPrimaryResults();
                Map<Object, String> actualResults = new HashMap<>();
                while (resultSet.next()) {
                    Object keyObject = resultSet.getObject(keyColumn);
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
}