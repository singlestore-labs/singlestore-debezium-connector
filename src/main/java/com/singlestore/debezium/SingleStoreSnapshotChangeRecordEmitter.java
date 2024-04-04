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

public class SingleStoreSnapshotChangeRecordEmitter extends
    SnapshotChangeRecordEmitter<SingleStorePartition> {

  private static final String INTERNAL_ID = "internalId";

  private final long internalId;

  public SingleStoreSnapshotChangeRecordEmitter(SingleStorePartition partition,
      OffsetContext offset, Object[] row, long internalId, Clock clock,
      RelationalDatabaseConnectorConfig connectorConfig) {
    super(partition, offset, row, clock, connectorConfig);
    this.internalId = internalId;
  }

  @Override
  protected void emitReadRecord(Receiver<SingleStorePartition> receiver, TableSchema tableSchema)
      throws InterruptedException {
    Object[] newColumnValues = getNewColumnValues();
    Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
    Struct envelope = tableSchema.getEnvelopeSchema()
        .read(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

    receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.READ, keyFromInternalId(),
        envelope, getOffset(), null);
  }

  private Struct keyFromInternalId() {
    Struct result = new Struct(
        SchemaBuilder.struct().field(INTERNAL_ID, Schema.INT64_SCHEMA).build());
    result.put(INTERNAL_ID, internalId);
    return result;
  }
}
