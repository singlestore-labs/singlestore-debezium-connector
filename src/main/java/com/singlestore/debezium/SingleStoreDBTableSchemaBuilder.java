package com.singlestore.debezium;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

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

public class SingleStoreDBTableSchemaBuilder extends TableSchemaBuilder {

    private static final String INTERNAL_ID = "internalId";

    public SingleStoreDBTableSchemaBuilder(ValueConverterProvider valueConverterProvider,
            DefaultValueConverter defaultValueConverter, SchemaNameAdjuster schemaNameAdjuster,
            CustomConverterRegistry customConverterRegistry, Schema sourceInfoSchema, FieldNamer<Column> fieldNamer,
            boolean multiPartitionMode) {
        super(valueConverterProvider, defaultValueConverter, schemaNameAdjuster, customConverterRegistry, sourceInfoSchema,
                fieldNamer, multiPartitionMode);
    }
    
    public TableSchema create(TopicNamingStrategy topicNamingStrategy, Table table, ColumnNameFilter filter, ColumnMappers mappers, KeyMapper keysMapper) {
        TableSchema schema = super.create(topicNamingStrategy, table, filter, mappers, keysMapper);
        if (schema.keySchema() == null) {
            return new TableSchema(schema.id(), 
                SchemaBuilder.struct().field(INTERNAL_ID, Schema.INT64_SCHEMA).build(),
                (row) -> schema.keyFromColumnData(row),
                schema.getEnvelopeSchema(),
                schema.valueSchema(),
                (row) -> schema.valueFromColumnData(row));
        } else {
            return schema;
        }
    }
}
