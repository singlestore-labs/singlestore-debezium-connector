package com.singlestore.debezium;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;

import java.sql.SQLException;

/**
 * Component that records the schema information for the {@link SingleStoreConnector}. The schema
 * information contains the {@link io.debezium.relational.Tables table definitions} and the Kafka
 * Connect {@link #schemaFor(TableId) Schema}s for each table, where the
 * {@link org.apache.kafka.connect.data.Schema} excludes any columns that have been
 * {@link SingleStoreConnectorConfig#COLUMN_EXCLUDE_LIST specified} in the configuration.
 */
public class SingleStoreDatabaseSchema extends RelationalDatabaseSchema {

  public SingleStoreDatabaseSchema(SingleStoreConnectorConfig config,
      SingleStoreValueConverters valueConverter,
      SingleStoreDefaultValueConverter defaultValueConverter,
      TopicNamingStrategy<TableId> topicNamingStrategy,
      boolean tableIdCaseInsensitive) {
    super(config, topicNamingStrategy, config.getTableFilters().dataCollectionFilter(),
        config.getColumnFilter(),
        getTableSchemaBuilder(config, valueConverter, defaultValueConverter),
        tableIdCaseInsensitive, config.getKeyMapper());
  }

  private static TableSchemaBuilder getTableSchemaBuilder(SingleStoreConnectorConfig config,
      SingleStoreValueConverters valueConverter,
      SingleStoreDefaultValueConverter defaultValueConverter) {
    return new SingleStoreTableSchemaBuilder(valueConverter, defaultValueConverter,
        config.schemaNameAdjuster(),
        config.customConverterRegistry(), config.getSourceInfoStructMaker().schema(),
        config.getFieldNamer(), false, config.populateInternalId());
  }

  /**
   * Initializes the content for this schema by reading all the database information from the
   * supplied connection.
   *
   * @param connection a {@link JdbcConnection} instance, never {@code null}
   * @return this object so methods can be chained together; never null
   * @throws SQLException if there is a problem obtaining the schema from the database server
   */
  protected SingleStoreDatabaseSchema refresh(SingleStoreConnection connection)
      throws SQLException {
    connection.readSchema(tables(), null, null, getTableFilter(), null, true);
    refreshSchemas();
    return this;
  }

  /**
   * Discard any currently-cached schemas and rebuild them using the filters.
   */
  protected void refreshSchemas() {
    clearSchemas();
    tableIds().forEach(this::refreshSchema);
  }
}
