package com.singlestore.debezium;

import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Component that records the schema information for the {@link SingleStoreDBConnector}. The schema information contains
 * the {@link Tables table definitions} and the Kafka Connect {@link #schemaFor(TableId) Schema}s for each table, where the
 * {@link Schema} excludes any columns that have been {@link SingleStoreDBConnectorConfig#COLUMN_EXCLUDE_LIST specified} in the
 * configuration.
 */
public class SingleStoreDBDatabaseSchema extends RelationalDatabaseSchema {
    public SingleStoreDBDatabaseSchema(SingleStoreDBConnectorConfig connectorConfig, 
    SingleStoreDBDefaultValueConverter defaultValueConverter, TopicNamingStrategy<TableId> topicNamingStrategy,
    SchemaNameAdjuster schemaNameAdjuster, boolean tableIdCaseInsensitive, SingleStoreDBValueConverters valueConverter) {
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
