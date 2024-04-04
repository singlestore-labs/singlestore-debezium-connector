package com.singlestore.debezium;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.singlestore.debezium.SingleStoreConnectorConfig.SecureConnectionMode;
import com.singlestore.debezium.SingleStoreConnectorConfig.SnapshotMode;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.DefaultUnicodeTopicNamingStrategy;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

public class SingleStoreConnectorIT extends IntegrationTestBase {

  @Test
  public void shouldFailToValidateInvalidConfiguration() {
    Configuration config = Configuration.create().build();
    SingleStoreConnector connector = new SingleStoreConnector();
    Config result = connector.validate(config.asMap());

    assertConfigurationErrors(result, SingleStoreConnectorConfig.DATABASE_NAME, 1);
    assertConfigurationErrors(result, SingleStoreConnectorConfig.TABLE_NAME, 1);
    assertConfigurationErrors(result, SingleStoreConnectorConfig.HOSTNAME, 1);
    assertConfigurationErrors(result, SingleStoreConnectorConfig.USER, 1);
    assertConfigurationErrors(result, CommonConnectorConfig.TOPIC_PREFIX, 1);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.PORT);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.TOPIC_NAMING_STRATEGY);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.COLUMN_EXCLUDE_LIST);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.COLUMN_INCLUDE_LIST);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.CONNECTION_TIMEOUT_MS);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.MAX_QUEUE_SIZE);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.MAX_BATCH_SIZE);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.POLL_INTERVAL_MS);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.SNAPSHOT_MODE);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.SSL_MODE);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.SSL_KEYSTORE);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.SSL_KEYSTORE_PASSWORD);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.SSL_TRUSTSTORE);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.DECIMAL_HANDLING_MODE);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.TIME_PRECISION_MODE);
    assertNoConfigurationErrors(result, SingleStoreConnectorConfig.POPULATE_INTERNAL_ID);
  }

  @Test
  public void shouldValidateAcceptableConfiguration() {
    Configuration config = Configuration.create().build();
    SingleStoreConnector connector = new SingleStoreConnector();
    Config result = connector.validate(config.asMap());

    // validate that the required fields have errors
    assertConfigurationErrors(result, SingleStoreConnectorConfig.DATABASE_NAME, 1);
    assertConfigurationErrors(result, SingleStoreConnectorConfig.TABLE_NAME, 1);
    assertConfigurationErrors(result, SingleStoreConnectorConfig.HOSTNAME, 1);
    assertConfigurationErrors(result, SingleStoreConnectorConfig.USER, 1);
    assertConfigurationErrors(result, CommonConnectorConfig.TOPIC_PREFIX, 1);

    // validate the non required fields
    validateConfigField(result, SingleStoreConnectorConfig.PORT, 3306);
    validateConfigField(result, SingleStoreConnectorConfig.PASSWORD, null);
    validateConfigField(result, SingleStoreConnectorConfig.COLUMN_EXCLUDE_LIST, null);
    validateConfigField(result, SingleStoreConnectorConfig.COLUMN_INCLUDE_LIST, null);
    validateConfigField(result, SingleStoreConnectorConfig.CONNECTION_TIMEOUT_MS, 30000);
    validateConfigField(result, SingleStoreConnectorConfig.MAX_QUEUE_SIZE, 8192);
    validateConfigField(result, SingleStoreConnectorConfig.MAX_BATCH_SIZE, 2048);
    validateConfigField(result, SingleStoreConnectorConfig.POLL_INTERVAL_MS, 500L);
    validateConfigField(result, SingleStoreConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
    validateConfigField(result, SingleStoreConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLE);
    validateConfigField(result, SingleStoreConnectorConfig.SSL_KEYSTORE, null);
    validateConfigField(result, SingleStoreConnectorConfig.SSL_KEYSTORE_PASSWORD, null);
    validateConfigField(result, SingleStoreConnectorConfig.SSL_TRUSTSTORE, null);
    validateConfigField(result, SingleStoreConnectorConfig.SSL_TRUSTSTORE_PASSWORD, null);
    validateConfigField(result, SingleStoreConnectorConfig.DECIMAL_HANDLING_MODE,
        DecimalHandlingMode.PRECISE);
    validateConfigField(result, SingleStoreConnectorConfig.TIME_PRECISION_MODE,
        TemporalPrecisionMode.ADAPTIVE);
    validateConfigField(result, SingleStoreConnectorConfig.TOPIC_NAMING_STRATEGY,
        DefaultTopicNamingStrategy.class.getName());
    validateConfigField(result, SingleStoreConnectorConfig.POPULATE_INTERNAL_ID, false);
  }

  private <T> void validateConfigField(Config config, Field field, T expectedValue) {
    assertNoConfigurationErrors(config, field);
    Object actualValue = configValue(config, field.name()).value();
    if (actualValue == null) {
      actualValue = field.defaultValue();
    }
    if (expectedValue == null) {
      assertThat(actualValue).isNull();
    } else {
      if (expectedValue instanceof EnumeratedValue) {
        assertThat(((EnumeratedValue) expectedValue).getValue()).isEqualTo(actualValue.toString());
      } else {
        assertThat(expectedValue).isEqualTo(actualValue);
      }
    }
  }

  @Test
  public void successfullConfigValidation() {
    SingleStoreConnector connector = new SingleStoreConnector();
    Config validatedConfig = connector.validate(defaultJdbcConfigWithTable("person").asMap());

    assertNoConfigurationErrors(validatedConfig, SingleStoreConnectorConfig.ALL_FIELDS.asArray());
  }

  @Test
  public void configWrongCredentials() {
    SingleStoreConnector connector = new SingleStoreConnector();
    Map<String, String> config = defaultJdbcConfigWithTable("person").asMap();
    config.put("database.hostname", "wrongHost");

    Config validatedConfig = connector.validate(config);
    assertConfigurationErrors(validatedConfig, SingleStoreConnectorConfig.HOSTNAME, 1);
  }

  @Test
  public void configMissingDB() {
    SingleStoreConnector connector = new SingleStoreConnector();
    Map<String, String> config = defaultJdbcConfigWithTable("person").asMap();
    config.put("database.dbname", null);

    Config validatedConfig = connector.validate(config);
    assertConfigurationErrors(validatedConfig, SingleStoreConnectorConfig.DATABASE_NAME, 1);
  }

  @Test
  public void configMissingTable() {
    SingleStoreConnector connector = new SingleStoreConnector();
    Map<String, String> config = defaultJdbcConfigWithTable("person").asMap();
    config.put("database.table", null);

    Config validatedConfig = connector.validate(config);
    assertConfigurationErrors(validatedConfig, SingleStoreConnectorConfig.TABLE_NAME, 1);
  }


  @Test
  public void testTopicOptions() throws SQLException, InterruptedException {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      Configuration config = defaultJdbcConfigWithTable("product");
      config = config.edit()
          .with(SingleStoreConnectorConfig.TOPIC_PREFIX, "prefix")
          .with(AbstractTopicNamingStrategy.TOPIC_DELIMITER, "_")
          .build();

      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();

      try {
        conn.execute("INSERT INTO `product` (`id`) VALUES (1)");

        List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertEquals(1, records.size());
        assertEquals("prefix_db_product", records.get(0).topic());
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void testTopicNamingStrategy() throws SQLException, InterruptedException {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      Configuration config = defaultJdbcConfigWithTable("product");
      config = config.edit()
          .with(SingleStoreConnectorConfig.TOPIC_NAMING_STRATEGY,
              DefaultUnicodeTopicNamingStrategy.class.getName())
          .build();

      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForStreamingToStart();

      try {
        conn.execute("INSERT INTO `product` (`id`) VALUES (1)");

        List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertEquals(1, records.size());
        assertEquals("singlestore_u005ftopic.db.product", records.get(0).topic());
      } finally {
        stopConnector();
      }
    }
  }
}
