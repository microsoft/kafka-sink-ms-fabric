package com.microsoft.fabric.connect.eventhouse.sink.it.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    public static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry";
    public static final int SCHEMA_REGISTRY_PORT = 8081;
    public final String bootstrapHost;

    public SchemaRegistryContainer(String version, String bootstrapHost) {
        super(SCHEMA_REGISTRY_IMAGE + ":" + version);
        this.bootstrapHost = bootstrapHost;
        this.withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "PLAINTEXT")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapHost);
        this.waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        this.withExposedPorts(SCHEMA_REGISTRY_PORT);
    }
}
