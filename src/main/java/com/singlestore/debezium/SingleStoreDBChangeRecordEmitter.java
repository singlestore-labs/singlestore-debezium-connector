package com.singlestore.debezium;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

public class SingleStoreDBChangeRecordEmitter extends RelationalChangeRecordEmitter<SingleStoreDBPartition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleStoreDBSnapshotChangeRecordEmitter.class);
    private final Envelope.Operation operation;
    private final OffsetContext offset;
    private final Object[] before;
    private final Object[] after;
    private final long internalId;
    
    private static final String INTERNAL_ID = "internalId";

    public SingleStoreDBChangeRecordEmitter(SingleStoreDBPartition partition, OffsetContext offset, Clock clock, Operation operation, Object[] before,
                                    Object[] after, long internalId, SingleStoreDBConnectorConfig connectorConfig) {
        super(partition, offset, clock, connectorConfig);
        this.offset = offset;
        this.operation = operation;
        this.before = before;
        this.after = after;
        this.internalId = internalId;
    }

    @Override
    protected void emitCreateRecord(Receiver<SingleStoreDBPartition> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = getKey(tableSchema, newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().create(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            // This case can be hit on UPDATE / DELETE when there's no primary key defined while using certain decoders
            LOGGER.warn("no new values found for table '{}' from create message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }
        receiver.changeRecord(getPartition(), tableSchema, Operation.CREATE, newKey, envelope, getOffset(), null);
    }

    @Override
    protected void emitUpdateRecord(Receiver<SingleStoreDBPartition> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();

        Struct newKey = getKey(tableSchema, newColumnValues);

        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            LOGGER.debug("no new values found for table '{}' from update message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }

        /*
         * If skip.messages.without.change is configured true,
         * Skip Publishing the message in case there is no change in monitored columns
         * (Postgres) Only works if REPLICA IDENTITY is set to FULL - as oldValues won't be available
         */
        if (skipMessagesWithoutChange() && Objects.nonNull(newValue) && newValue.equals(oldValue)) {
            LOGGER.debug("No new values found for table '{}' in included columns from update message at '{}'; skipping record", tableSchema,
                    getOffset().getSourceInfo());
            return;
        }

        Struct envelope = tableSchema.getEnvelopeSchema().update(oldValue, newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.UPDATE, newKey, envelope, getOffset(), null);
    }

    @Override
    protected void emitDeleteRecord(Receiver<SingleStoreDBPartition> receiver, TableSchema tableSchema) throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = getKey(tableSchema, newColumnValues);

        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (oldColumnValues == null || oldColumnValues.length == 0)) {
            LOGGER.warn("no old values found for table '{}' from delete message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }

        Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.DELETE, newKey, envelope, getOffset(), null);
    }

    @Override
    public OffsetContext getOffset() {
        return offset;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return before;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return after;
    }

    private Struct getKey(TableSchema tableSchema, Object[] columnData) {
        Struct key = tableSchema.keyFromColumnData(columnData);
        if (key == null) {
            return keyFromInternalId();
        } else {
            return key;
        }
    }

    private Struct keyFromInternalId() {
        Struct result = new Struct(SchemaBuilder.struct().field(INTERNAL_ID, Schema.INT64_SCHEMA).build());
        result.put(INTERNAL_ID, internalId);
        return result;
    }
}
