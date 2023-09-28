package com.singlestore.debezium;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

public class SingleStoreDBOffsetContext extends CommonOffsetContext<SourceInfo> {

    private final Schema sourceInfoSchema;

    public SingleStoreDBOffsetContext(SingleStoreDBConnectorConfig connectorConfig) {
        super(new SourceInfo(connectorConfig));

        sourceInfo.update(null, null, null, null);
        sourceInfoSchema = sourceInfo.schema();
    }

    public static class Loader implements OffsetContext.Loader<SingleStoreDBOffsetContext> {

        private final SingleStoreDBConnectorConfig connectorConfig;

        public Loader(SingleStoreDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public SingleStoreDBOffsetContext load(Map<String, ?> offset) {
            // TODO: implement
            return new SingleStoreDBOffsetContext(connectorConfig);
        }
    }



    @Override
    public Map<String, ?> getOffset() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOffset'");
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot();
    }

    @Override
    public void preSnapshotStart() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'preSnapshotStart'");
    }

    @Override
    public void preSnapshotCompletion() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'preSnapshotCompletion'");
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'event'");
    }

    @Override
    public TransactionContext getTransactionContext() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTransactionContext'");
    }
    
}
