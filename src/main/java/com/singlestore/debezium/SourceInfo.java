package com.singlestore.debezium;

import java.time.Instant;

import io.debezium.connector.common.BaseSourceInfo;

public class SourceInfo extends BaseSourceInfo {

    public SourceInfo(SingleStoreDBConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    protected Instant timestamp() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'timestamp'");
    }

    @Override
    protected String database() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'database'");
    }
}
