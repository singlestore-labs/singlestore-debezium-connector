package com.singlestore.debezium;

import java.util.ArrayList;
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

public class SingleStoreDBTableSchemaBuilder extends TableSchemaBuilder {

    public static final String INTERNAL_ID = "internalId";
    private final Boolean populateInternalId;

    public SingleStoreDBTableSchemaBuilder(ValueConverterProvider valueConverterProvider,
            DefaultValueConverter defaultValueConverter, SchemaNameAdjuster schemaNameAdjuster,
            CustomConverterRegistry customConverterRegistry, Schema sourceInfoSchema, FieldNamer<Column> fieldNamer,
            boolean multiPartitionMode, Boolean populateInternalId) {
        super(valueConverterProvider, defaultValueConverter, schemaNameAdjuster, customConverterRegistry, sourceInfoSchema,
                fieldNamer, multiPartitionMode);
        this.populateInternalId = populateInternalId;
    }

    private Schema addInternalId(Schema s) {
        SchemaBuilder res = SchemaBuilder.struct();
        for (Field f: s.fields()) {
            res.field(f.name(), f.schema());
        }
        res.field(INTERNAL_ID, Schema.INT64_SCHEMA);

        return res.optional().build();
    }

    private final List<Column> addInternalId(List<Column> columns) {
        List<Column> result = new ArrayList<>(columns);    
        result.add(Column.editor()
            .name(INTERNAL_ID)
            .position(result.size()+1)
            .type("INT64")
            .autoIncremented(false)
            .generated(false)
            .optional(false)
            .jdbcType(-5)
            .nativeType(-1)
            .length(19)
            .scale(0)
            .create());

        return result;
    }
     
    public TableSchema create(TopicNamingStrategy topicNamingStrategy, Table table, ColumnNameFilter filter, ColumnMappers mappers, KeyMapper keysMapper) {
        TableSchema schema = super.create(topicNamingStrategy, table, filter, mappers, keysMapper);

        if (!populateInternalId) {
            return new TableSchema(schema.id(), 
            SchemaBuilder.struct().field(INTERNAL_ID, Schema.INT64_SCHEMA).build(),
            (row) -> schema.keyFromColumnData(row),
            schema.getEnvelopeSchema(),
            schema.valueSchema(),
            (row) -> schema.valueFromColumnData(row));
        } else {
            Schema valSchema = addInternalId(schema.valueSchema());
            List<Column> valColumns = addInternalId(table.columns());
            Envelope envelopeWithoutInternalId = schema.getEnvelopeSchema();
            Envelope envelope = Envelope.defineSchema()
                    .withName(envelopeWithoutInternalId.schema().name())
                    .withRecord(valSchema)
                    .withSource(envelopeWithoutInternalId.schema().field("source").schema())
                    .build();
            
            return new TableSchema(schema.id(), 
                SchemaBuilder.struct().field(INTERNAL_ID, Schema.INT64_SCHEMA).build(),
                (row) -> schema.keyFromColumnData(row),
                envelope,
                valSchema,
                createValueGenerator(valSchema, table.id(), valColumns, filter, mappers));
        }
    }
}
