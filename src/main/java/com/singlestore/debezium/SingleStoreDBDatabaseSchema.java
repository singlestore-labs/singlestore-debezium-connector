package com.singlestore.debezium;

import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

// TODO: think how to emit schema history
public class SingleStoreDBDatabaseSchema extends HistorizedRelationalDatabaseSchema {
    public SingleStoreDBDatabaseSchema(SingleStoreDBConnectorConfig connectorConfig, 
    SingleStoreDBDefaultValueConverter defaultValueConverter, TopicNamingStrategy<TableId> topicNamingStrategy,
    SchemaNameAdjuster schemaNameAdjuster, boolean tableIdCaseInsensitive, SingleStoreDBValueConverter valueConverter) {
        super(connectorConfig, topicNamingStrategy, connectorConfig.getTableFilters().dataCollectionFilter(), connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverter,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false),
                tableIdCaseInsensitive, connectorConfig.getKeyMapper());            
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'applySchemaChange'");
    }

    @Override
    protected DdlParser getDdlParser() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDdlParser'");
    }
}
