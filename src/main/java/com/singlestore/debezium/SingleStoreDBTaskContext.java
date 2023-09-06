package com.singlestore.debezium;

import io.debezium.connector.common.CdcSourceTaskContext;

public class SingleStoreDBTaskContext extends CdcSourceTaskContext {
    public SingleStoreDBTaskContext(SingleStoreDBConnectorConfig config, SingleStoreDBDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
    }
}
