package com.singlestore.debezium;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Bits;
import io.debezium.data.VerifyRecord;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.time.Year;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SingleStoreDBDatabaseSchemaIT extends IntegrationTestBase {

    private static final SingleStoreDBValueConverters CONVERTERS = new SingleStoreDBValueConverters(JdbcValueConverters.DecimalMode.DOUBLE,
            TemporalPrecisionMode.CONNECT, CommonConnectorConfig.BinaryHandlingMode.BYTES);
    private SingleStoreDBDatabaseSchema schema;

    @Test
    public void testKeySchema() {
        schema = getSchema(new SingleStoreDBConnectorConfig(defaultJdbcConfig()));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
            schema.refresh(conn);
            assertKeySchema("db.allTypesTable", "intColumn", SchemaBuilder.int32().defaultValue(2147483647).optional().build());//as unique index
            assertKeySchema("db.person", "name", SchemaBuilder.string().required().build());
            assertKeySchema("db.product", "id", SchemaBuilder.int32().required().build());
            assertKeySchema("db.purchased", "productId,purchaser",
                    SchemaBuilder.int32().required().build(), SchemaBuilder.string().required().build());
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testTableSchema() {
        schema = getSchema(new SingleStoreDBConnectorConfig(defaultJdbcConfig()));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
            schema.refresh(conn);
            assertTablesIncluded("db.person", "db.product", "db.purchased", "db.allTypesTable");
            assertTableSchema("db.person", "name, birthdate, age, salary, bitStr",
                    SchemaBuilder.string().required().build(),
                    org.apache.kafka.connect.data.Date.builder().optional().build(),
                    SchemaBuilder.int32().defaultValue(10).optional().build(),
                    SchemaBuilder.float64().optional().build(),
                    Bits.builder(18).optional().build()
            );
            assertTableSchema("db.product", "id, createdByDate, modifiedDate",
                    SchemaBuilder.int32().required().build(),//id
                    org.apache.kafka.connect.data.Timestamp.builder()
                            .defaultValue(Date.from(LocalDateTime.of(1970, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC))).required().build(), //createdByDate epoch timestamp
                    org.apache.kafka.connect.data.Timestamp.builder()
                            .defaultValue(Date.from(LocalDateTime.of(1970, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC))).required().build() //modifiedDate epoch timestamp
            );
            assertTableSchema("db.allTypesTable", "boolColumn, booleanColumn, bitColumn, tinyintColumn, mediumintColumn, " +
                            "smallintColumn, intColumn, integerColumn, bigintColumn, floatColumn, doubleColumn, realColumn, dateColumn, timeColumn, " +
                            "time6Column, datetimeColumn, datetime6Column, timestampColumn, timestamp6Column, yearColumn, decimalColumn, decColumn, " +
                            "fixedColumn, numericColumn, charColumn, mediumtextColumn, binaryColumn, varcharColumn, varbinaryColumn, longtextColumn, " +
                            "textColumn, tinytextColumn, longblobColumn, mediumblobColumn, blobColumn, tinyblobColumn, jsonColumn, enum_f, set_f, geographyColumn, geographypointColumn",
                    Bits.builder(3).optional().defaultValue("1".getBytes(StandardCharsets.UTF_8)).build(), //boolColumn
                    Bits.builder(3).optional().defaultValue("1".getBytes(StandardCharsets.UTF_8)).build(), //booleanColumn
                    Bits.builder(64).optional().defaultValue("01234567".getBytes(StandardCharsets.UTF_8)).build(), //bitColumn
                    SchemaBuilder.int16().optional().defaultValue((short) 124).build(), //tinyintColumn
                    SchemaBuilder.int32().optional().defaultValue(8388607).build(), //mediumintColumn
                    SchemaBuilder.int16().optional().defaultValue((short) 32767).build(),//smallintColumn
                    SchemaBuilder.int32().optional().defaultValue(2147483647).build(),//intColumn
                    SchemaBuilder.int32().optional().defaultValue(2147483647).build(),//integerColumn
                    SchemaBuilder.int64().optional().defaultValue(9223372036854775807L).build(), //bigintColumn
                    SchemaBuilder.float32().optional().defaultValue(10.1f).build(), //floatColumn
                    SchemaBuilder.float64().optional().defaultValue(100.1).build(), //doubleColumn
                    SchemaBuilder.float64().optional().defaultValue(100.1).build(), //realColumn
                    org.apache.kafka.connect.data.Date.builder().optional()
                            .defaultValue(Date.from(LocalDate.of(2000, 10, 10).atStartOfDay(ZoneId.of("UTC")).toInstant())).build(), //dateColumn
                    org.apache.kafka.connect.data.Time.builder().optional()
                            .defaultValue(java.util.Date.from(LocalDate.EPOCH.atTime(22, 59, 59).toInstant(ZoneOffset.UTC))).build(),//timeColumn
                    org.apache.kafka.connect.data.Time.builder().optional()
                            .defaultValue(java.util.Date.from(LocalDate.EPOCH.atTime(22, 59, 59, 111111000).toInstant(ZoneOffset.UTC))).build(),//time6Column
                    org.apache.kafka.connect.data.Timestamp.builder().optional()
                            .defaultValue(Date.from(LocalDateTime.of(2023, 12, 31, 23, 59, 59).atZone(ZoneId.of("UTC")).toInstant())).build(), //datetimeColumn
                    org.apache.kafka.connect.data.Timestamp.builder().optional()
                            .defaultValue(Date.from(LocalDateTime.of(2023, 12, 31, 22, 59, 59, 111111000).atZone(ZoneId.of("UTC")).toInstant())).build(), //datetime6Column
                    org.apache.kafka.connect.data.Timestamp.builder().optional()
                            .defaultValue(Date.from(LocalDateTime.of(2022, 1, 19, 3, 14, 7).atZone(ZoneId.of("UTC")).toInstant())).build(), //timestampColumn
                    org.apache.kafka.connect.data.Timestamp.builder().optional()
                            .defaultValue(Date.from(LocalDateTime.of(2022, 1, 19, 3, 14, 7).atZone(ZoneId.of("UTC")).toInstant())).build(), //timestamp6Column
                    Year.builder().optional().defaultValue(1989).build(), //yearColumn
                    SchemaBuilder.float64().defaultValue(10000.100001).optional().build(), //decimalColumn
                    SchemaBuilder.float64().defaultValue(10000.100001).optional().build(), //decColumn
                    SchemaBuilder.float64().defaultValue(10000.100001).optional().build(), //fixedColumn
                    SchemaBuilder.float64().defaultValue(10000.100001).optional().build(), //numericColumn
                    SchemaBuilder.string().optional().defaultValue("a").build(), //charColumn
                    SchemaBuilder.string().optional().defaultValue("abc").build(), //mediumtextColumn
                    SchemaBuilder.bytes().optional().defaultValue("a".getBytes(StandardCharsets.UTF_8)).build(), //binaryColumn
                    SchemaBuilder.string().optional().defaultValue("abc").build(), //varcharColumn
                    SchemaBuilder.bytes().optional().defaultValue("abc".getBytes(StandardCharsets.UTF_8)).build(), //varbinaryColumn
                    SchemaBuilder.string().optional().defaultValue("abc").build(), //longtextColumn
                    SchemaBuilder.string().optional().defaultValue("abc").build(), //textColumn
                    SchemaBuilder.string().optional().defaultValue("abc").build(), //tinytextColumn
                    SchemaBuilder.bytes().optional().defaultValue("abc".getBytes(StandardCharsets.UTF_8)).build(), //longblobColumn
                    SchemaBuilder.bytes().optional().defaultValue("abc".getBytes(StandardCharsets.UTF_8)).build(), //mediumblobColumn
                    SchemaBuilder.bytes().optional().defaultValue("abc".getBytes(StandardCharsets.UTF_8)).build(), //blobColumn
                    SchemaBuilder.bytes().optional().defaultValue("abc".getBytes(StandardCharsets.UTF_8)).build(), //tinyblobColumn
                    io.debezium.data.Json.builder().optional().defaultValue("{}").build(), //jsonColumn
                    SchemaBuilder.string().optional().defaultValue("val1").build(), //enum_f
                    SchemaBuilder.string().optional().defaultValue("v1").build(), //set_f
                    io.debezium.data.geometry.Geometry.builder().optional().build(),//geographyColumn
                    io.debezium.data.geometry.Geometry.builder().optional().build()//geographypointColumn
            );
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testUpdateTableSchema() {
        String statements = "CREATE DATABASE IF NOT EXISTS d3; " +
                "DROP TABLE IF EXISTS d3.A;" +
                "DROP TABLE IF EXISTS d3.B;" +
                "DROP TABLE IF EXISTS d3.C;" +
                "CREATE ROWSTORE TABLE d3.A (pk INT, aa VARCHAR(10), ab INT, PRIMARY KEY(pk));" +
                "CREATE TABLE d3.B (pk INT, aa VARCHAR(10), PRIMARY KEY(pk));";
        execute(statements);
        Configuration configuration = defaultJdbcConfigBuilder().with(SingleStoreDBConnectorConfig.DATABASE_INCLUDE_LIST, "d3").build();
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertTablesIncluded("d3.A", "d3.B");
            assertTableSchema("d3.A", "pk, aa",
                    SchemaBuilder.int32().required().defaultValue(0).build(),
                    SchemaBuilder.string().optional().build()
            );
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        String updateStatements = "DROP TABLE d3.B;" +
                "CREATE TABLE d3.C(pk INT, aa VARCHAR(10), PRIMARY KEY(pk));" +
                "ALTER TABLE d3.A MODIFY COLUMN ab DOUBLE;" +
                "ALTER TABLE d3.A DROP COLUMN aa;" +
                "ALTER TABLE d3.A ADD COLUMN ac CHAR(1) default 'a';";
        execute(updateStatements);
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertTablesIncluded("d3.A", "d3.C");
            assertTablesExcluded("d3.B");
            assertTableSchema("d3.A", "pk, ab, ac",
                    SchemaBuilder.int32().required().defaultValue(0).build(),
                    SchemaBuilder.float64().optional().build(),
                    SchemaBuilder.string().optional().defaultValue("a").build()
            );
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testApplyFilters() {
        String statements = "CREATE DATABASE IF NOT EXISTS d1; " +
                "CREATE DATABASE IF NOT EXISTS d2; " +
                "DROP TABLE IF EXISTS d1.A;" +
                "DROP TABLE IF EXISTS d1.B;" +
                "DROP TABLE IF EXISTS d2.A;" +
                "DROP TABLE IF EXISTS d2.B;" +
                "CREATE TABLE d1.A (pk INT, aa VARCHAR(10), PRIMARY KEY(pk));" +
                "CREATE TABLE d1.B (pk INT, ba VARCHAR(10), PRIMARY KEY(pk));" +
                "CREATE TABLE d2.A (pk INT, aa VARCHAR(10), PRIMARY KEY(pk));" +
                "CREATE TABLE d2.B (pk INT, ba VARCHAR(10), PRIMARY KEY(pk));";
        execute(statements);
        Configuration configuration = defaultJdbcConfigBuilder().with(SingleStoreDBConnectorConfig.DATABASE_EXCLUDE_LIST, "d1").build();
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertTablesIncluded("d2.A", "d2.B");
            assertTablesExcluded("d1.A", "d1.B");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        configuration = defaultJdbcConfigBuilder().with(SingleStoreDBConnectorConfig.DATABASE_EXCLUDE_LIST, "d.*").build();
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertTablesExcluded("d1.A", "d1.B", "d2.A", "d2.B");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        configuration = defaultJdbcConfigBuilder().with(SingleStoreDBConnectorConfig.TABLE_EXCLUDE_LIST, "d1.A,d2.A").build();
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertTablesIncluded("d1.B", "d2.B");
            assertTablesExcluded("d1.A", "d2.A");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        configuration = defaultJdbcConfigBuilder().with(SingleStoreDBConnectorConfig.DATABASE_EXCLUDE_LIST, "d2")
                .with(SingleStoreDBConnectorConfig.TABLE_EXCLUDE_LIST, "d1.A").build();
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertTablesIncluded("d1.B");
            assertTablesExcluded("d1.A", "d2.A", "d2.A");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        configuration = defaultJdbcConfigBuilder().with(SingleStoreDBConnectorConfig.COLUMN_EXCLUDE_LIST, ".*aa").build();
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertColumnsExcluded("d1.a.aa", "d2.b.aa");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        configuration = defaultJdbcConfigBuilder().with(SingleStoreDBConnectorConfig.COLUMN_EXCLUDE_LIST, ".*aa").build();
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration));
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertColumnsExcluded("d1.a.aa", "d2.a.aa");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        //test tableIdCaseInsensitive
        configuration = defaultJdbcConfigBuilder().build();
        schema = getSchema(new SingleStoreDBConnectorConfig(configuration), false);
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(configuration))) {
            schema.refresh(conn);
            assertTablesExcluded("d1.b", "d2.b", "d1.a", "d2.a");
            assertTablesIncluded("d1.B", "d2.B", "d1.A", "d2.A");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    public static SingleStoreDBDatabaseSchema getSchema(SingleStoreDBConnectorConfig config, boolean tableIdCaseInsensitive) {
        return new SingleStoreDBDatabaseSchema(
                config,
                CONVERTERS,
                new SingleStoreDBDefaultValueConverter(CONVERTERS),
                config.getTopicNamingStrategy(SingleStoreDBConnectorConfig.TOPIC_NAMING_STRATEGY),
                tableIdCaseInsensitive);
    }

    public static SingleStoreDBDatabaseSchema getSchema(SingleStoreDBConnectorConfig config) {
        return getSchema(config, true);
    }

    protected void assertTablesIncluded(String... fullyQualifiedTableNames) {
        Arrays.stream(fullyQualifiedTableNames).forEach(fullyQualifiedTableName -> {
            TableSchema tableSchema = schemaFor(fullyQualifiedTableName);
            assertNotNull(fullyQualifiedTableName + " not included", tableSchema);
            assertThat(tableSchema.keySchema().name()).isEqualTo(validFullName(fullyQualifiedTableName, ".Key"));
            assertThat(tableSchema.valueSchema().name()).isEqualTo(validFullName(fullyQualifiedTableName, ".Value"));
        });
    }

    protected void assertKeySchema(String fullyQualifiedTableName, String fields, Schema... expectedSchemas) {
        TableSchema tableSchema = schemaFor(fullyQualifiedTableName);
        Schema keySchema = tableSchema.keySchema();
        assertSchemaContent(keySchema, fields.split(","), expectedSchemas);
    }

    protected void assertTableSchema(String fullyQualifiedTableName, String fields, Schema... expectedSchemas) {
        TableSchema tableSchema = schemaFor(fullyQualifiedTableName);
        Schema valueSchema = tableSchema.valueSchema();
        assertSchemaContent(valueSchema, fields.split(","), expectedSchemas);
    }

    private void assertSchemaContent(Schema actualSchema, String[] fields, Schema[] expectedSchemas) {
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].trim();

            Field field = actualSchema.field(Strings.unquoteIdentifierPart(fieldName));
            assertNotNull(fieldName + " not found in schema", field);
            VerifyRecord.assertConnectSchemasAreEqual(fieldName, field.schema(), expectedSchemas[i]);
        }
    }

    protected void assertTablesExcluded(String... fullyQualifiedTableNames) {
        Arrays.stream(fullyQualifiedTableNames).forEach(fullyQualifiedTableName -> {
            assertThat(tableFor(fullyQualifiedTableName)).isNull();
            assertThat(schemaFor(fullyQualifiedTableName)).isNull();
        });
    }

    protected void assertColumnsExcluded(String... columnNames) {
        Arrays.stream(columnNames).forEach(fqColumnName -> {
            int lastDotIdx = fqColumnName.lastIndexOf(".");
            String fullyQualifiedTableName = fqColumnName.substring(0, lastDotIdx);
            String columnName = lastDotIdx > 0 ? fqColumnName.substring(lastDotIdx + 1) : fqColumnName;
            TableSchema tableSchema = schemaFor(fullyQualifiedTableName);
            assertNotNull(fullyQualifiedTableName + " not included", tableSchema);
            Schema valueSchema = tableSchema.valueSchema();
            assertNotNull(fullyQualifiedTableName + ".Value schema not included", valueSchema);
            assertNull(columnName + " not excluded;", valueSchema.field(columnName));
        });
    }

    private Table tableFor(String fqn) {
        return schema.tableFor(TableId.parse(fqn, true));
    }

    private String validFullName(String proposedName, String suffix) {
        TableId id = TableId.parse(proposedName, true);
        return SchemaNameAdjuster.validFullname(TEST_TOPIC_PREFIX + "." + id.catalog() + "." + id.table() + suffix);
    }

    protected TableSchema schemaFor(String fqn) {
        Table table = tableFor(fqn);
        return table != null ? schema.schemaFor(table.id()) : null;
    }
}
