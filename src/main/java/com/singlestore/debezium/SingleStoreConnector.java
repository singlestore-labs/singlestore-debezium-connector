package com.singlestore.debezium;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * A Kafka Connect source connector that creates tasks that read the SingleStore change log and
 * generate the corresponding data change events.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in
 * {@link SingleStoreConnectorConfig}.
 */

public class SingleStoreConnector extends RelationalBaseSourceConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleStoreConnector.class);

  @Immutable
  private Map<String, String> properties;

  public SingleStoreConnector() {
  }

  @Override
  public String version() {
    return Module.version();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SingleStoreConnectorTask.class;
  }

  @Override
  public void start(Map<String, String> props) {
    this.properties = Collections.unmodifiableMap(new HashMap<>(props));
  }


  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    if (maxTasks > 1) {
      throw new IllegalArgumentException("Only a single connector task may be started");
    }

    return Collections.singletonList(properties);
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return SingleStoreConnectorConfig.configDef();
  }

  @Override
  protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
    ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
    // Try to connect to the database ...
    final SingleStoreConnection.SingleStoreConnectionConfiguration connectionConfig =
        new SingleStoreConnection.SingleStoreConnectionConfiguration(config);
    try (SingleStoreConnection connection = new SingleStoreConnection(connectionConfig)) {
      try {
        connection.connect();
        connection.execute("SELECT 1");
        LOGGER.info("Successfully tested connection for {} with user '{}'",
            connection.connectionString(), connectionConfig.username());
      } catch (SQLException e) {
        LOGGER.error("Failed testing connection for {} with user '{}'",
            connection.connectionString(), connectionConfig.username(), e);
        hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
      }
    } catch (SQLException e) {
      LOGGER.error("Unexpected error shutting down the database connection", e);
    }
  }

  @Override
  protected Map<String, ConfigValue> validateAllFields(Configuration config) {
    return config.validate(SingleStoreConnectorConfig.ALL_FIELDS);
  }

  @Override
  public List<TableId> getMatchingCollections(Configuration config) {
    SingleStoreConnectorConfig connectorConfig = new SingleStoreConnectorConfig(config);
    final SingleStoreConnection.SingleStoreConnectionConfiguration connectionConfig =
        new SingleStoreConnection.SingleStoreConnectionConfiguration(config);
    try (SingleStoreConnection connection = new SingleStoreConnection(connectionConfig)) {
      return connection.readTableNames(connectorConfig.databaseName(), null, null,
              new String[]{"TABLE"}).stream()
          .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter()
              .isIncluded(tableId))
          .collect(Collectors.toList());
    } catch (SQLException e) {
      throw new DebeziumException(e);
    }
  }
}
