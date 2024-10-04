package com.singlestore.debezium;

import com.singlestore.debezium.util.InternalIdUtils;
import io.debezium.relational.Table;
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

public class SingleStoreChangeRecordEmitter extends
    RelationalChangeRecordEmitter<SingleStorePartition> {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      SingleStoreSnapshotChangeRecordEmitter.class);
  private final Envelope.Operation operation;
  private final OffsetContext offset;
  private final Object[] before;
  private final Object[] after;
  private final String internalId;
  private final Table table;

  public SingleStoreChangeRecordEmitter(SingleStorePartition partition, OffsetContext offset,
      Clock clock, Operation operation, Object[] before,
      Object[] after, String internalId, SingleStoreConnectorConfig connectorConfig, Table table) {
    super(partition, offset, clock, connectorConfig);
    this.offset = offset;
    this.operation = operation;
    this.before = before;
    this.after = after;
    this.internalId = internalId;
    this.table = table;
  }

  @Override
  protected void emitCreateRecord(Receiver<SingleStorePartition> receiver, TableSchema tableSchema)
      throws InterruptedException {
    Object[] newColumnValues = getNewColumnValues();
    Struct newKey = InternalIdUtils.generateKey(table, tableSchema, newColumnValues, internalId);
    Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
    Struct envelope = tableSchema.getEnvelopeSchema()
        .create(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

    if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
      // This case can be hit on UPDATE / DELETE when there's no primary key defined while using certain decoders
      LOGGER.warn("no new values found for table '{}' from create message at '{}'; skipping record",
          tableSchema, getOffset().getSourceInfo());
      return;
    }
    receiver.changeRecord(getPartition(), tableSchema, Operation.CREATE, newKey, envelope,
        getOffset(), null);
  }

  @Override
  protected void emitUpdateRecord(Receiver<SingleStorePartition> receiver, TableSchema tableSchema)
      throws InterruptedException {
    Object[] oldColumnValues = getOldColumnValues();
    Object[] newColumnValues = getNewColumnValues();

    Struct newKey = InternalIdUtils.generateKey(table, tableSchema, newColumnValues, internalId);

    Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
    Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

    if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
      LOGGER.debug(
          "no new values found for table '{}' from update message at '{}'; skipping record",
          tableSchema, getOffset().getSourceInfo());
      return;
    }

    /*
     * If skip.messages.without.change is configured true,
     * Skip Publishing the message in case there is no change in monitored columns
     * (Postgres) Only works if REPLICA IDENTITY is set to FULL - as oldValues won't be available
     */
    if (skipMessagesWithoutChange() && Objects.nonNull(newValue) && newValue.equals(oldValue)) {
      LOGGER.debug(
          "No new values found for table '{}' in included columns from update message at '{}'; skipping record",
          tableSchema,
          getOffset().getSourceInfo());
      return;
    }

    Struct envelope = tableSchema.getEnvelopeSchema()
        .update(oldValue, newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
    receiver.changeRecord(getPartition(), tableSchema, Operation.UPDATE, newKey, envelope,
        getOffset(), null);
  }

  @Override
  protected void emitDeleteRecord(Receiver<SingleStorePartition> receiver, TableSchema tableSchema)
      throws InterruptedException {
    Object[] oldColumnValues = getOldColumnValues();
    Object[] newColumnValues = getNewColumnValues();
    Struct newKey = InternalIdUtils.generateKey(table, tableSchema, newColumnValues, internalId);

    Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

    if (skipEmptyMessages() && (oldColumnValues == null || oldColumnValues.length == 0)) {
      LOGGER.warn("no old values found for table '{}' from delete message at '{}'; skipping record",
          tableSchema, getOffset().getSourceInfo());
      return;
    }

    Struct envelope = tableSchema.getEnvelopeSchema()
        .delete(oldValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
    receiver.changeRecord(getPartition(), tableSchema, Operation.DELETE, newKey, envelope,
        getOffset(), null);
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
}
