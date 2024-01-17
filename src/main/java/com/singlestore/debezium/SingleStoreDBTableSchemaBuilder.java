package com.singlestore.debezium;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
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

    private static final String INTERNAL_ID = "InternalId";

    public SingleStoreDBTableSchemaBuilder(ValueConverterProvider valueConverterProvider,
            DefaultValueConverter defaultValueConverter, SchemaNameAdjuster schemaNameAdjuster,
            CustomConverterRegistry customConverterRegistry, Schema sourceInfoSchema, FieldNamer<Column> fieldNamer,
            boolean multiPartitionMode) {
        super(valueConverterProvider, defaultValueConverter, schemaNameAdjuster, customConverterRegistry, sourceInfoSchema,
                fieldNamer, multiPartitionMode);
    }

    private Schema addInternalId(Schema s) {
        SchemaBuilder res = SchemaBuilder.struct();
        for (Field f: s.fields()) {
            res.field(f.name(), f.schema());
        }
        res.field(INTERNAL_ID, Schema.INT64_SCHEMA);

        return res.optional().build();
    }
    
    public TableSchema create(TopicNamingStrategy topicNamingStrategy, Table table, ColumnNameFilter filter, ColumnMappers mappers, KeyMapper keysMapper) {
        
        TableSchema schema = super.create(topicNamingStrategy, table, filter, mappers, keysMapper);
        Schema valSchema = addInternalId(schema.valueSchema());
        List<Column> valColumns = new ArrayList<>(table.columns());

        ColumnEditor e = Column.editor();
        e.name(INTERNAL_ID);
        e.position(valColumns.size()+1);
        e.type("INT64");
        e.autoIncremented(false);
        e.generated(false);
        e.optional(false);
        e.jdbcType(-5);
        e.nativeType(-1);
        e.length(19);
        e.scale(0);
        valColumns.add(e.create());

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
