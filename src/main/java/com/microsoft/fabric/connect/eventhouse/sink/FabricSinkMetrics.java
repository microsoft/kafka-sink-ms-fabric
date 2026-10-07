package com.microsoft.fabric.connect.eventhouse.sink;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMX metrics for the Fabric Sink Connector (same counters as Azure/kafka-sink-azure-kusto's KustoSinkMetrics).
 * Each task registers its own MBean:
 * {@code com.microsoft.fabric.connect.eventhouse.sink:type=FabricSinkMetrics,connector=<name>,task=<n>}
 */
public class FabricSinkMetrics implements FabricSinkMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(FabricSinkMetrics.class);
    static final String MBEAN_DOMAIN = "com.microsoft.fabric.connect.eventhouse.sink";
    private static final AtomicInteger TASK_SEQUENCE = new AtomicInteger();

    private final AtomicLong recordsWritten = new AtomicLong();
    private final AtomicLong recordsFailed = new AtomicLong();
    private final AtomicLong ingestionAttempts = new AtomicLong();
    private final AtomicLong ingestionSuccesses = new AtomicLong();
    private final AtomicLong ingestionFailures = new AtomicLong();
    private final AtomicLong dlqRecordsSent = new AtomicLong();

    private ObjectName objectName;

    public FabricSinkMetrics(String connectorName) {
        register(connectorName == null ? "unknown" : connectorName);
    }

    private void register(String connectorName) {
        try {
            ObjectName name = new ObjectName(String.format("%s:type=FabricSinkMetrics,connector=%s,task=%d",
                    MBEAN_DOMAIN, ObjectName.quote(connectorName), TASK_SEQUENCE.getAndIncrement()));
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, name);
            objectName = name;
            LOGGER.info("Registered JMX MBean: {}", name);
        } catch (JMException e) {
            LOGGER.warn("Failed to register JMX MBean for connector {}", connectorName, e);
        }
    }

    ObjectName getObjectName() {
        return objectName;
    }

    /**
     * Unregisters this task's MBean from JMX.
     */
    public void close() {
        if (objectName == null) {
            return;
        }
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (mbs.isRegistered(objectName)) {
                mbs.unregisterMBean(objectName);
                LOGGER.info("Unregistered JMX MBean: {}", objectName);
            }
        } catch (JMException e) {
            LOGGER.warn("Failed to unregister JMX MBean: {}", objectName, e);
        }
    }

    public void incrementRecordsWritten() {
        recordsWritten.incrementAndGet();
    }

    public void incrementRecordsFailed() {
        recordsFailed.incrementAndGet();
    }

    public void incrementIngestionAttempts() {
        ingestionAttempts.incrementAndGet();
    }

    public void incrementIngestionSuccesses() {
        ingestionSuccesses.incrementAndGet();
    }

    public void incrementIngestionFailures() {
        ingestionFailures.incrementAndGet();
    }

    public void incrementDlqRecordsSent() {
        dlqRecordsSent.incrementAndGet();
    }

    @Override
    public long getRecordsWritten() {
        return recordsWritten.get();
    }

    @Override
    public long getRecordsFailed() {
        return recordsFailed.get();
    }

    @Override
    public long getIngestionAttempts() {
        return ingestionAttempts.get();
    }

    @Override
    public long getIngestionSuccesses() {
        return ingestionSuccesses.get();
    }

    @Override
    public long getIngestionFailures() {
        return ingestionFailures.get();
    }

    @Override
    public long getDlqRecordsSent() {
        return dlqRecordsSent.get();
    }
}
