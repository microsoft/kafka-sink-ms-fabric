package com.microsoft.fabric.connect.eventhouse.sink;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {
    public static final String CLIENT_NAME = "EventHouse.Kafka.Sink";
    public static final String APP_NAME = "Fabric.Kafka.Sink";

    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String VERSION_FILE = "/ms-eventhouse-kafka-sink-version.properties";
    private static String connectorVersion = "unknown";

    private Version() {
    }

    static {
        try {
            Properties props = new Properties();
            try (InputStream versionFileStream = Version.class.getResourceAsStream(VERSION_FILE)) {
                props.load(versionFileStream);
                connectorVersion = props.getProperty("version", connectorVersion).trim();
            }
        } catch (Exception e) {
            log.warn("Error while loading version:", e);
        }
    }

    public static String getConnectorVersion() {
        return connectorVersion;
    }
}
