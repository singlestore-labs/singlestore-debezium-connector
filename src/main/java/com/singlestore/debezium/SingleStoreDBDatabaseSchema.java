package com.singlestore.debezium;

import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

// TODO: think how to emit schema history
public class SingleStoreDBDatabaseSchema extends RelationalDatabaseSchema {
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
}
