package com.singlestore.debezium;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleStoreDBSnapshotChangeRecordEmitter extends SnapshotChangeRecordEmitter<SingleStoreDBPartition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleStoreDBSnapshotChangeRecordEmitter.class);
    private static final String INTERNAL_ID = "internalId";

    private final long internalId;

    public SingleStoreDBSnapshotChangeRecordEmitter(SingleStoreDBPartition partition, OffsetContext offset, Object[] row, long internalId, Clock clock, RelationalDatabaseConnectorConfig connectorConfig) {
        super(partition, offset, row, clock, connectorConfig);
        this.internalId = internalId;
    }

    @Override
    protected void emitCreateRecord(Receiver<SingleStoreDBPartition> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().create(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            // This case can be hit on UPDATE / DELETE when there's no primary key defined while using certain decoders
            LOGGER.warn("no new values found for table '{}' from create message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }
        receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.CREATE, newKey != null ? newKey : keyFromInternalId(), envelope, getOffset(), null);
    }

    @Override
    protected void emitReadRecord(Receiver<SingleStoreDBPartition> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().read(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.READ, newKey != null ? newKey : keyFromInternalId(), envelope, getOffset(), null);
    }

    private Struct keyFromInternalId() {
        Struct result = new Struct(SchemaBuilder.struct().field(INTERNAL_ID, Schema.INT64_SCHEMA).build());
        result.put(INTERNAL_ID, internalId);
        return result;
    }
}
