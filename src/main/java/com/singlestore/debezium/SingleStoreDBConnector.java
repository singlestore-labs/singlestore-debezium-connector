package com.singlestore.debezium;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;


import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;

/**
 * A Kafka Connect source connector that creates tasks that read the SingleStoreDB change log and generate the corresponding
 * data change events.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link SingleStoreDBConnectorConfig}.
 */

public class SingleStoreDBConnector extends RelationalBaseSourceConnector {

    @Immutable
    private Map<String, String> properties;

    public SingleStoreDBConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SingleStoreDBConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }


    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }

        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return SingleStoreDBConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        // TODO: implement connection validation logic
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(SingleStoreDBConnectorConfig.ALL_FIELDS);
    }
}
