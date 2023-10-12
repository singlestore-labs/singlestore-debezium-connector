package com.singlestore.debezium;

import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.junit.Test;

public class SingleStoreDBConnectorIT extends IntegrationTestBase {
    
    @Test
    public void successfullConfigValidation() {
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Config validatedConfig = connector.validate(defaultJdbcConfig().asMap());

        assertNoConfigurationErrors(validatedConfig, SingleStoreDBConnectorConfig.ALL_FIELDS.asArray());
    }

    @Test
    public void configWrongCredentials() {
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Map<String, String> config = defaultJdbcConfig().asMap();
        config.put("database.hostname", "wrongHost");

        Config validatedConfig = connector.validate(config);
        assertConfigurationErrors(validatedConfig, SingleStoreDBConnectorConfig.HOSTNAME, 1);
    }
}
