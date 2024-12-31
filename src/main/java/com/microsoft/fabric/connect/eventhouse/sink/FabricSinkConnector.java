package com.microsoft.fabric.connect.eventhouse.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FabricSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(FabricSinkConnector.class);
    private FabricSinkConfig config;

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting FabricSinkConnector.");
        config = new FabricSinkConfig(props);
        config.validateConfig();
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
        return FabricSinkConfig.getConfig();
    }

    @Override
    public Class<? extends Task> taskClass() {
        if(config.getFabricTarget() == FabricSinkConfig.FabricTarget.EVENTHOUSE) {
            return EventHouseSinkTask.class;
        } else {
            throw new NotImplementedException("EventStream fabric target not implemented.");
        }
    }

    @Override
    public String version() {
        return Version.getConnectorVersion();
    }
}
