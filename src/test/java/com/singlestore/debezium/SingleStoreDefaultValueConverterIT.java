package com.singlestore.debezium;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class SingleStoreDefaultValueConverterIT extends IntegrationTestBase {

  private static final SingleStoreValueConverters CONVERTERS = new SingleStoreValueConverters(
      JdbcValueConverters.DecimalMode.DOUBLE,
      TemporalPrecisionMode.ADAPTIVE, CommonConnectorConfig.BinaryHandlingMode.BYTES);

  @Test
  public void testNumberValues() {
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      SingleStoreDefaultValueConverter defaultValueConverter = new SingleStoreDefaultValueConverter(
          CONVERTERS);
      testColumn(defaultValueConverter, table, "tinyintColumn", (short) 124);
      testColumn(defaultValueConverter, table, "smallintColumn", (short) 32767);
      testColumn(defaultValueConverter, table, "mediumintColumn", 8388607);
      testColumn(defaultValueConverter, table, "bigintColumn", 9223372036854775807L);
      testColumn(defaultValueConverter, table, "intColumn", 2147483647);
      testColumn(defaultValueConverter, table, "doubleColumn", 100.1d);
      testColumn(defaultValueConverter, table, "realColumn", 100.1d);
      testColumn(defaultValueConverter, table, "floatColumn", 10.1f);
      testColumn(defaultValueConverter, table, "yearColumn", 1989);
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDefaultTimeAndDateValues() {
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      SingleStoreDefaultValueConverter defaultValueConverter = new SingleStoreDefaultValueConverter(
          CONVERTERS);
      testColumn(defaultValueConverter, table, "dateColumn",
          (int) LocalDate.of(2000, 10, 10).atStartOfDay(ZoneId.of("UTC")).toEpochSecond() / 60 / 60
              / 24);//epoch days
      testColumn(defaultValueConverter, table, "timeColumn",
          Date.from(LocalDate.EPOCH.atTime(22, 59, 59).atZone(ZoneId.of("UTC")).toInstant())
              .getTime());
      testColumn(defaultValueConverter, table, "time6Column",
          Date.from(LocalDate.EPOCH.atTime(22, 59, 59, 111111).atZone(ZoneId.of("UTC")).toInstant())
              .getTime() * 1_000 + 111111);
      testColumn(defaultValueConverter, table, "datetimeColumn",
          Date.from(LocalDateTime.of(2023, 12, 31, 23, 59, 59).atZone(ZoneId.of("UTC")).toInstant())
              .getTime());
      testColumn(defaultValueConverter, table, "datetime6Column",
          Date.from(LocalDateTime.of(2023, 12, 31, 22, 59, 59, 111111).atZone(ZoneId.of("UTC"))
              .toInstant()).getTime() * 1_000 + 111111);
      testColumn(defaultValueConverter, table, "timestampColumn",
          Date.from(LocalDateTime.of(2022, 1, 19, 3, 14, 7).atZone(ZoneId.of("UTC")).toInstant())
              .getTime());
//            testColumn(defaultValueConverter, table, "timestamp6Column",
//                    Date.from(LocalDateTime.of(2022, 1, 19, 3, 14, 7, 111111).atZone(ZoneId.of("UTC")).toInstant()).getTime() * 1_000 + 111111); //todo uncomment after DB issued with defaultValue is fixed: DB-65291
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  @Ignore //todo enable after PLAT-6817 is resolved
  public void testGeometryValues() throws ParseException {
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      SingleStoreDefaultValueConverter defaultValueConverter = new SingleStoreDefaultValueConverter(
          CONVERTERS);
      Column geographyColumn = table.columnWithName("geographyColumn");
      Optional<Object> geographyDefaultValue = defaultValueConverter.parseDefaultValue(
          geographyColumn, geographyColumn.defaultValueExpression().orElse(null));
      assertTrue(geographyDefaultValue.isPresent());
      SingleStoreGeometry geographyValue = SingleStoreGeometry.fromEkt(
          "POLYGON((1 1,2 1,2 2, 1 2, 1 1))");
      Struct geographyColumnDefaultValue = (Struct) geographyDefaultValue.get();
      assertArrayEquals(geographyValue.getWkb(), (byte[]) geographyColumnDefaultValue.get("wkb"));
      Column geographypointColumn = table.columnWithName("geographypointColumn");
      Optional<Object> geographypointDefaultValue = defaultValueConverter.parseDefaultValue(
          geographypointColumn, geographypointColumn.defaultValueExpression().orElse(null));
      assertTrue(geographypointDefaultValue.isPresent());
      SingleStoreGeometry geographyPointValue = SingleStoreGeometry.fromEkt(
          "POINT(1.50000003 1.50000000)");
      Struct geographypointColumnDefaultValue = (Struct) geographypointDefaultValue.get();
      assertArrayEquals(geographyPointValue.getWkb(),
          (byte[]) geographypointColumnDefaultValue.get("wkb"));
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testStringValues() {
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      SingleStoreDefaultValueConverter defaultValueConverter = new SingleStoreDefaultValueConverter(
          CONVERTERS);
      testColumn(defaultValueConverter, table, "jsonColumn", "{}");
      testColumn(defaultValueConverter, table, "enum_f", "val1");
      testColumn(defaultValueConverter, table, "set_f", "v1");
      testColumn(defaultValueConverter, table, "tinytextColumn", "abc");
      testColumn(defaultValueConverter, table, "longtextColumn", "abc");
      testColumn(defaultValueConverter, table, "textColumn", "abc");
      testColumn(defaultValueConverter, table, "varbinaryColumn",
          ByteBuffer.wrap("abc".getBytes()));
      testColumn(defaultValueConverter, table, "charColumn", "a");
      testColumn(defaultValueConverter, table, "binaryColumn", ByteBuffer.wrap("a".getBytes()));
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testBlobValues() {
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      SingleStoreDefaultValueConverter defaultValueConverter = new SingleStoreDefaultValueConverter(
          CONVERTERS);
      testColumn(defaultValueConverter, table, "blobColumn", ByteBuffer.wrap("abc".getBytes()));
      testColumn(defaultValueConverter, table, "longblobColumn", ByteBuffer.wrap("abc".getBytes()));
      testColumn(defaultValueConverter, table, "mediumblobColumn",
          ByteBuffer.wrap("abc".getBytes()));
      testColumn(defaultValueConverter, table, "tinyblobColumn", ByteBuffer.wrap("abc".getBytes()));
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  private static void testColumn(SingleStoreDefaultValueConverter defaultValueConverter,
      Table table, String name, Object expectedValue) {
    Column column = table.columnWithName(name);
    Optional<Object> defaultValue = defaultValueConverter.parseDefaultValue(column,
        column.defaultValueExpression().orElse(null));
    assertTrue(defaultValue.isPresent());
    assertEquals(expectedValue, defaultValue.get());
  }
}
