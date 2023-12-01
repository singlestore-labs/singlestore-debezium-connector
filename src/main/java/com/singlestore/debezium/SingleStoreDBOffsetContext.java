package com.singlestore.debezium;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SingleStoreDBOffsetContext extends CommonOffsetContext<SourceInfo> {

    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    /**
     * Whether a snapshot has been completed or not.
     */
    private boolean snapshotCompleted;
    private final Schema sourceInfoSchema;

    public SingleStoreDBOffsetContext(SingleStoreDBConnectorConfig connectorConfig, Integer partitionId, 
        String txId, List<String> offsets, boolean snapshot, boolean snapshotCompleted) {
        super(new SourceInfo(connectorConfig, offsets.size()));

        sourceInfo.update(partitionId, txId, offsets);
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        } else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    public static SingleStoreDBOffsetContext initial(SingleStoreDBConnectorConfig connectorConfig, Supplier<Integer> partitionNumberSupplier) {
        int numPartitions = partitionNumberSupplier.get();
        if (numPartitions < 1) {
            throw new IllegalArgumentException("Wrong number of partitions: " + numPartitions);
        }
        ArrayList<String> initOffsetList = Stream.generate(() -> (String) null)
                .limit(numPartitions).collect(Collectors.toCollection(ArrayList::new));
        return new SingleStoreDBOffsetContext(connectorConfig, null, null, initOffsetList, true, false);
    }

    public static class Loader implements OffsetContext.Loader<SingleStoreDBOffsetContext> {

        private final SingleStoreDBConnectorConfig connectorConfig;

        public Loader(SingleStoreDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        private List<String> parseOffsets(String offsets) {
            if (offsets == null) {
                return null;
            }

            return Arrays.asList(offsets.split(",")).stream().map(offset -> {
                if (offset.equals("null")) {
                    return null;
                } else {
                    return offset;
                }
            }).collect(Collectors.toList());
        }

        @Override
        public SingleStoreDBOffsetContext load(Map<String, ?> offset) {
            String txId = (String) offset.get(SourceInfo.TXID_KEY);
            Integer partitionId = (Integer) offset.get(SourceInfo.PARTITIONID_KEY);
            List<String> offsets = parseOffsets((String) offset.get(SourceInfo.OFFSETS_KEY));
            Boolean snapshot = (Boolean) ((Map<String, Object>) offset).getOrDefault(SourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
            Boolean snapshotCompleted = (Boolean) ((Map<String, Object>) offset).getOrDefault(SNAPSHOT_COMPLETED_KEY, Boolean.FALSE);

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

    public List<String> offsets() {
        return sourceInfo.offsets();
    }

    public Integer partitionId() {
        return sourceInfo.partitionId();
    }

    public String txId() {
        return sourceInfo.txId();
    }

    public Instant timestamp() {
        return sourceInfo.timestamp();
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

    public void update(Integer partitionId, String txId, String offset) {
        sourceInfo.update(partitionId, txId, offset);
    }

    public void update(Integer partitionId, String txId, List<String> offsets) {
        sourceInfo.update(partitionId, txId, offsets);
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.update((TableId) collectionId, timestamp);
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
