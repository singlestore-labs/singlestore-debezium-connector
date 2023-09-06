package com.singlestore.debezium;

import io.debezium.config.Configuration;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;

public class SingleStoreDBConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {
    public SingleStoreDBConnectorConfig(Configuration config) {
        super(SingleStoreDBConnectorConfig.class,
            config,
            null,
            true,
            
        )
    }

    
}
