package com.singlestore.debezium;

import io.debezium.connector.common.CdcSourceTaskContext;

public class SingleStoreTaskContext extends CdcSourceTaskContext {
    public SingleStoreTaskContext(SingleStoreConnectorConfig config, SingleStoreDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), config.getCustomMetricTags(), schema::tableIds);
    }
}
