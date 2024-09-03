package com.microsoft.fabric.kafka.connect.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.fabric.kafka.connect.sink.es.EventStreamSinkConfig;
import com.microsoft.fabric.kafka.connect.sink.es.EventStreamSinkTask;
import com.microsoft.fabric.kafka.connect.sink.eventhouse.EventHouseSinkConfig;
import com.microsoft.fabric.kafka.connect.sink.eventhouse.EventHouseSinkTask;

public class FabricSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(FabricSinkConnector.class);
    private SinkType sinkType = SinkType.KQLDB;
    private AbstractConfig config;


    @Override
    public void start(@NotNull Map<String, String> props) {
        String connectionString = props.get("connection.string");
        LOGGER.info("Starting FabricSinkConnector");
        if(StringUtils.isNotEmpty(connectionString) && connectionString.startsWith("Endpoint=sb:")) {
            LOGGER.info("Detected EventStream connector using EventStream target.");
            sinkType = SinkType.EVENTSTREAM;
            config = new EventStreamSinkConfig(props);
        } else {
            LOGGER.info("Detected Eventhouse URL , using KQLDb as a target.");
            sinkType = SinkType.KQLDB;
            config = new EventHouseSinkConfig(props);
        }
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks == 0) {
            LOGGER.warn("No Connector tasks have been configured.");
        }
        List<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
        for (int i = 0; i < maxTasks; i++) {
            configs.add(taskProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        LOGGER.info("Shutting down FabricSinkConnector");
    }

    @Override
    public ConfigDef config() {
        if(sinkType == SinkType.EVENTSTREAM) {
            return EventStreamSinkConfig.CONFIG_DEF;
        }
        else {
            return EventHouseSinkConfig.CONFIG_DEF;
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        if(sinkType == SinkType.EVENTSTREAM) {
            return EventStreamSinkTask.class;
        }
        else {
            return EventHouseSinkTask.class;
        }
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
    enum SinkType {
        KQLDB,
        EVENTSTREAM
    }
}
