package com.singlestore.debezium;

import com.singlestore.debezium.util.InternalIdUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.protocol.types.Field.Bool;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.schema.FieldNameSelector.FieldNamer;

public class SingleStoreTableSchemaBuilder extends TableSchemaBuilder {

  private final Boolean populateInternalId;

  public SingleStoreTableSchemaBuilder(ValueConverterProvider valueConverterProvider,
      DefaultValueConverter defaultValueConverter, SchemaNameAdjuster schemaNameAdjuster,
      CustomConverterRegistry customConverterRegistry, Schema sourceInfoSchema,
      FieldNamer<Column> fieldNamer,
      boolean multiPartitionMode, Boolean populateInternalId) {
    super(valueConverterProvider, defaultValueConverter, schemaNameAdjuster,
        customConverterRegistry, sourceInfoSchema,
        fieldNamer, multiPartitionMode);
    this.populateInternalId = populateInternalId;
  }

  public TableSchema create(TopicNamingStrategy topicNamingStrategy, Table table,
      ColumnNameFilter filter, ColumnMappers mappers, KeyMapper keysMapper) {
    TableSchema schema = super.create(topicNamingStrategy, table, filter, mappers, keysMapper);
    Schema keySchema = InternalIdUtils.getKeySchema(table, schema);

    if (!populateInternalId) {
      return new TableSchema(schema.id(),
          keySchema,
          (row) -> schema.keyFromColumnData(row),
          schema.getEnvelopeSchema(),
          schema.valueSchema(),
          (row) -> schema.valueFromColumnData(row));
    } else {
      Schema valSchema = InternalIdUtils.addInternalId(schema.valueSchema());
      List<Column> valColumns = InternalIdUtils.addInternalId(table.columns());
      Envelope envelopeWithoutInternalId = schema.getEnvelopeSchema();
      Envelope envelope = Envelope.defineSchema()
          .withName(envelopeWithoutInternalId.schema().name())
          .withRecord(valSchema)
          .withSource(envelopeWithoutInternalId.schema().field("source").schema())
          .build();

      return new TableSchema(schema.id(),
          keySchema,
          (row) -> schema.keyFromColumnData(row),
          envelope,
          valSchema,
          createValueGenerator(valSchema, table.id(), valColumns, filter, mappers));
    }
  }
}
