package com.singlestore.debezium;

import com.singlestore.debezium.util.InternalIdUtils;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class SingleStoreSnapshotChangeRecordEmitter extends
    SnapshotChangeRecordEmitter<SingleStorePartition> {

  private final long internalId;
  private final Table table;

  public SingleStoreSnapshotChangeRecordEmitter(SingleStorePartition partition,
      OffsetContext offset, Object[] row, long internalId, Clock clock,
      RelationalDatabaseConnectorConfig connectorConfig, Table table) {
    super(partition, offset, row, clock, connectorConfig);
    this.internalId = internalId;
    this.table = table;
  }

  @Override
  protected void emitReadRecord(Receiver<SingleStorePartition> receiver, TableSchema tableSchema)
      throws InterruptedException {
    Object[] newColumnValues = getNewColumnValues();
    Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
    Struct envelope = tableSchema.getEnvelopeSchema()
        .read(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

    receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.READ,
        InternalIdUtils.generateKey(table, tableSchema, newColumnValues, internalId),
        envelope, getOffset(), null);
  }
}
