package com.singlestore.debezium;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.singlestore.debezium.SingleStoreDBConnectorConfig.SecureConnectionMode;
import com.singlestore.debezium.SingleStoreDBConnectorConfig.SnapshotMode;
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

public class SingleStoreDBConnectorIT extends IntegrationTestBase {

    @Test
    public void shouldFailToValidateInvalidConfiguration() {
        Configuration config = Configuration.create().build();
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Config result = connector.validate(config.asMap());

        assertConfigurationErrors(result, SingleStoreDBConnectorConfig.DATABASE_NAME, 1);
        assertConfigurationErrors(result, SingleStoreDBConnectorConfig.TABLE_NAME, 1);
        assertConfigurationErrors(result, SingleStoreDBConnectorConfig.HOSTNAME, 1);
        assertConfigurationErrors(result, SingleStoreDBConnectorConfig.USER, 1);
        assertConfigurationErrors(result, CommonConnectorConfig.TOPIC_PREFIX, 1);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.PORT);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.TOPIC_NAMING_STRATEGY);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.COLUMN_EXCLUDE_LIST);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.COLUMN_INCLUDE_LIST);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.CONNECTION_TIMEOUT_MS);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.SNAPSHOT_MODE);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.SSL_MODE);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.SSL_KEYSTORE);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.SSL_KEYSTORE_PASSWORD);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.SSL_TRUSTSTORE);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.DECIMAL_HANDLING_MODE);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.TIME_PRECISION_MODE);
        assertNoConfigurationErrors(result, SingleStoreDBConnectorConfig.POPULATE_INTERNAL_ID);
    }

    @Test
    public void shouldValidateAcceptableConfiguration() {
        Configuration config = Configuration.create().build();
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Config result = connector.validate(config.asMap());

        // validate that the required fields have errors
        assertConfigurationErrors(result, SingleStoreDBConnectorConfig.DATABASE_NAME, 1);
        assertConfigurationErrors(result, SingleStoreDBConnectorConfig.TABLE_NAME, 1);
        assertConfigurationErrors(result, SingleStoreDBConnectorConfig.HOSTNAME, 1);
        assertConfigurationErrors(result, SingleStoreDBConnectorConfig.USER, 1);
        assertConfigurationErrors(result, CommonConnectorConfig.TOPIC_PREFIX, 1);

        // validate the non required fields
        validateConfigField(result, SingleStoreDBConnectorConfig.PORT, 3306);
        validateConfigField(result, SingleStoreDBConnectorConfig.PASSWORD, null);
        validateConfigField(result, SingleStoreDBConnectorConfig.COLUMN_EXCLUDE_LIST, null);
        validateConfigField(result, SingleStoreDBConnectorConfig.COLUMN_INCLUDE_LIST, null);
        validateConfigField(result, SingleStoreDBConnectorConfig.CONNECTION_TIMEOUT_MS, 30000);
        validateConfigField(result, SingleStoreDBConnectorConfig.MAX_QUEUE_SIZE, 8192);
        validateConfigField(result, SingleStoreDBConnectorConfig.MAX_BATCH_SIZE, 2048);
        validateConfigField(result, SingleStoreDBConnectorConfig.POLL_INTERVAL_MS, 500L);
        validateConfigField(result, SingleStoreDBConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
        validateConfigField(result, SingleStoreDBConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLE);
        validateConfigField(result, SingleStoreDBConnectorConfig.SSL_KEYSTORE, null);
        validateConfigField(result, SingleStoreDBConnectorConfig.SSL_KEYSTORE_PASSWORD, null);
        validateConfigField(result, SingleStoreDBConnectorConfig.SSL_TRUSTSTORE, null);
        validateConfigField(result, SingleStoreDBConnectorConfig.SSL_TRUSTSTORE_PASSWORD, null);
        validateConfigField(result, SingleStoreDBConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE);
        validateConfigField(result, SingleStoreDBConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE);
        validateConfigField(result, SingleStoreDBConnectorConfig.TOPIC_NAMING_STRATEGY, DefaultTopicNamingStrategy.class.getName());
        validateConfigField(result, SingleStoreDBConnectorConfig.POPULATE_INTERNAL_ID, false);
    }

    private <T> void validateConfigField(Config config, Field field, T expectedValue) {
        assertNoConfigurationErrors(config, field);
        Object actualValue = configValue(config, field.name()).value();
        if (actualValue == null) {
            actualValue = field.defaultValue();
        }
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        }
        else {
            if (expectedValue instanceof EnumeratedValue) {
                assertThat(((EnumeratedValue) expectedValue).getValue()).isEqualTo(actualValue.toString());
            }
            else {
                assertThat(expectedValue).isEqualTo(actualValue);
            }
        }
    }

    @Test
    public void successfullConfigValidation() {
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Config validatedConfig = connector.validate(defaultJdbcConfigWithTable("person").asMap());

        assertNoConfigurationErrors(validatedConfig, SingleStoreDBConnectorConfig.ALL_FIELDS.asArray());
    }

    @Test
    public void configWrongCredentials() {
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Map<String, String> config = defaultJdbcConfigWithTable("person").asMap();
        config.put("database.hostname", "wrongHost");

        Config validatedConfig = connector.validate(config);
        assertConfigurationErrors(validatedConfig, SingleStoreDBConnectorConfig.HOSTNAME, 1);
    }

    @Test
    public void configMissingDB() {
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Map<String, String> config = defaultJdbcConfigWithTable("person").asMap();
        config.put("database.dbname", null);

        Config validatedConfig = connector.validate(config);
        assertConfigurationErrors(validatedConfig, SingleStoreDBConnectorConfig.DATABASE_NAME, 1);
    }

    @Test
    public void configMissingTable() {
        SingleStoreDBConnector connector = new SingleStoreDBConnector();
        Map<String, String> config = defaultJdbcConfigWithTable("person").asMap();
        config.put("database.table", null);

        Config validatedConfig = connector.validate(config);
        assertConfigurationErrors(validatedConfig, SingleStoreDBConnectorConfig.TABLE_NAME, 1);
    }


    @Test
    public void testTopicOptions() throws SQLException, InterruptedException {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(
            defaultJdbcConnectionConfigWithTable("product"))) {
            Configuration config = defaultJdbcConfigWithTable("product");
            config = config.edit()
                .with(SingleStoreDBConnectorConfig.TOPIC_PREFIX, "prefix")
                .with(AbstractTopicNamingStrategy.TOPIC_DELIMITER, "_")
                .build();

            start(SingleStoreDBConnector.class, config);
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
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(
            defaultJdbcConnectionConfigWithTable("product"))) {
            Configuration config = defaultJdbcConfigWithTable("product");
            config = config.edit()
                .with(SingleStoreDBConnectorConfig.TOPIC_NAMING_STRATEGY, DefaultUnicodeTopicNamingStrategy.class.getName())

                .build();

            start(SingleStoreDBConnector.class, config);
            assertConnectorIsRunning();

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
