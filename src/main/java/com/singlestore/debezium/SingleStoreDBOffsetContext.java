package com.singlestore.debezium;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.protocol.types.Field.Bool;
import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.time.Conversions;

public class SingleStoreDBOffsetContext extends CommonOffsetContext<SourceInfo> {

    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    /**
     * Whether a snapshot has been completed or not.
     */
    private boolean snapshotCompleted;
    private final Schema sourceInfoSchema;

    public SingleStoreDBOffsetContext(SingleStoreDBConnectorConfig connectorConfig, Integer partitionId, 
        String txId, List<String> offsets, boolean snapshot, boolean snapshotCompleted) {
        super(new SourceInfo(connectorConfig));

        sourceInfo.update(null, partitionId, txId, offsets);
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    public static class Loader implements OffsetContext.Loader<SingleStoreDBOffsetContext> {

        private final SingleStoreDBConnectorConfig connectorConfig;

        public Loader(SingleStoreDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public SingleStoreDBOffsetContext load(Map<String, ?> offset) {
            String txId = (String) offset.get(SourceInfo.TXID_KEY);
            Integer partitionId = (Integer) offset.get(SourceInfo.PARTITIONID_KEY);
            List<String> offsets = Arrays.asList(((String) offset.get(SourceInfo.OFFSETS_KEY)).split(","));
            Boolean snapshot = (Boolean) offset.get(SourceInfo.SNAPSHOT_KEY);
            Boolean snapshotCompleted = (Boolean) offset.get(SNAPSHOT_COMPLETED_KEY);

            return new SingleStoreDBOffsetContext(connectorConfig, partitionId, txId, offsets, snapshot, snapshotCompleted);
        }
    }



    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();

        if (sourceInfo.txId() != null) {
            result.put(SourceInfo.TXID_KEY, sourceInfo.txId());
        }
        if (sourceInfo.offsets() != null) {
            result.put(SourceInfo.OFFSETS_KEY, sourceInfo.offsets().stream().collect(Collectors.joining(",")));
        }
        if (sourceInfo.isSnapshot()) {
            result.put(SourceInfo.SNAPSHOT_KEY, true);
        }
        if (sourceInfo.partitionId() != null) {
            result.put(SourceInfo.PARTITIONID_KEY, sourceInfo.partitionId());
        }
        result.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);

        return result;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.updateTable((TableId) collectionId);
        sourceInfo
    }

    @Override
    public TransactionContext getTransactionContext() {
        // TODO PLAT-6820 implement transaction monitoring 
        throw new UnsupportedOperationException("Unimplemented method 'getTransactionContext'");
    }

    @Override
    public String toString() {
        return "SqlServerOffsetContext [" +
        "sourceInfoSchema=" + sourceInfoSchema +
        ", sourceInfo=" + sourceInfo +
        ", snapshotCompleted=" + snapshotCompleted +
        "]";
    }
}
