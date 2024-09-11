package com.microsoft.fabric.kafka.connect.sink;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {
    public static final String CLIENT_NAME = "Fabric.Kafka.Sink";

    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String VERSION_FILE = "/microsoft-fabric-kafka-sink-version.properties";
    private static String version = "unknown";

    static {
        try {
            Properties props = new Properties();
            try (InputStream versionFileStream = Version.class.getResourceAsStream(VERSION_FILE)) {
                props.load(versionFileStream);
                version = props.getProperty("version", version).trim();
            }
        } catch (Exception e) {
            log.warn("Error while loading version:", e);
        }
    }

    public static String getVersion() {
        return version;
    }
}
