package com.microsoft.fabric.connect.eventhouse.sink;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FabricSinkMetricsTest {
    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

    @Test
    public void shouldRegisterAndUnregisterMBean() throws Exception {
        FabricSinkMetrics metrics = new FabricSinkMetrics("my-connector");
        ObjectName name = metrics.getObjectName();
        assertNotNull(name);
        assertEquals(FabricSinkMetrics.MBEAN_DOMAIN, name.getDomain());
        assertEquals("FabricSinkMetrics", name.getKeyProperty("type"));
        assertEquals("my-connector", ObjectName.unquote(name.getKeyProperty("connector")));
        assertTrue(MBS.isRegistered(name));
        metrics.close();
        assertFalse(MBS.isRegistered(name));
        assertDoesNotThrow(metrics::close);
    }

    @Test
    public void tasksOfSameConnectorShouldHaveSeparateMBeans() {
        FabricSinkMetrics task0 = new FabricSinkMetrics("shared");
        FabricSinkMetrics task1 = new FabricSinkMetrics("shared");
        try {
            assertNotEquals(task0.getObjectName(), task1.getObjectName());
            assertTrue(MBS.isRegistered(task0.getObjectName()));
            assertTrue(MBS.isRegistered(task1.getObjectName()));
            task0.close();
            assertTrue(MBS.isRegistered(task1.getObjectName()), "Closing one task must not unregister another");
        } finally {
            task0.close();
            task1.close();
        }
    }

    @Test
    public void shouldHandleNullAndSpecialConnectorNames() {
        FabricSinkMetrics nullName = new FabricSinkMetrics(null);
        FabricSinkMetrics special = new FabricSinkMetrics("a,b=c:d*\"e");
        try {
            assertNotNull(nullName.getObjectName());
            assertNotNull(special.getObjectName());
            assertEquals("a,b=c:d*\"e", ObjectName.unquote(special.getObjectName().getKeyProperty("connector")));
        } finally {
            nullName.close();
            special.close();
        }
    }

    @Test
    public void shouldExposeCountersThroughJmx() throws Exception {
        FabricSinkMetrics metrics = new FabricSinkMetrics("jmx-counters");
        try {
            metrics.incrementRecordsWritten();
            metrics.incrementRecordsWritten();
            metrics.incrementRecordsFailed();
            metrics.incrementIngestionAttempts();
            metrics.incrementIngestionSuccesses();
            metrics.incrementIngestionFailures();
            metrics.incrementDlqRecordsSent();
            ObjectName name = metrics.getObjectName();
            assertEquals(2L, MBS.getAttribute(name, "RecordsWritten"));
            assertEquals(1L, MBS.getAttribute(name, "RecordsFailed"));
            assertEquals(1L, MBS.getAttribute(name, "IngestionAttempts"));
            assertEquals(1L, MBS.getAttribute(name, "IngestionSuccesses"));
            assertEquals(1L, MBS.getAttribute(name, "IngestionFailures"));
            assertEquals(1L, MBS.getAttribute(name, "DlqRecordsSent"));
        } finally {
            metrics.close();
        }
    }
}
