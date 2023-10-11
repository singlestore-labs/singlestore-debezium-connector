package com.singlestore.debezium;

import org.apache.kafka.common.config.Config;
import org.junit.Test;

public class SingleStoreDBConnectorIT extends IntegrationTestBase {
    @Test
    public void successfullConnectionValidation() {
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Config res = connector.validate(defaultJdbcConfig().asMap());

        System.out.println(res);
    }

    @Test
    public void connectionValidationFailure() {

    }
}
