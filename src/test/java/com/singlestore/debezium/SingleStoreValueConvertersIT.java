package com.singlestore.debezium;

import com.singlestore.debezium.SingleStoreValueConverters.GeographyMode;
import com.singlestore.jdbc.SingleStoreBlob;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import io.debezium.util.HexConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.util.Base64;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SingleStoreValueConvertersIT extends IntegrationTestBase {

  private static final SingleStoreValueConverters CONVERTERS = new SingleStoreValueConverters(
      JdbcValueConverters.DecimalMode.DOUBLE, TemporalPrecisionMode.CONNECT,
      CommonConnectorConfig.BinaryHandlingMode.BYTES, GeographyMode.GEOMETRY);

  @Test
  public void testNumberValues() {
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      testColumn(CONVERTERS, table, "tinyintColumn", -128, (short) -128);
      testColumn(CONVERTERS, table, "tinyintColumn", 127, (short) 127);
      testColumn(CONVERTERS, table, "smallintColumn", -32768, (short) -32768);
      testColumn(CONVERTERS, table, "smallintColumn", 32767, (short) 32767);
      testColumn(CONVERTERS, table, "mediumintColumn", -8388608, -8388608);
      testColumn(CONVERTERS, table, "mediumintColumn", 8388607, 8388607);
      testColumn(CONVERTERS, table, "bigintColumn", Long.MAX_VALUE, Long.MAX_VALUE);
      testColumn(CONVERTERS, table, "bigintColumn", Long.MIN_VALUE, Long.MIN_VALUE);
      testColumn(CONVERTERS, table, "intColumn", -2147483648, -2147483648);
      testColumn(CONVERTERS, table, "intColumn", 2147483647, 2147483647);
      testColumn(CONVERTERS, table, "doubleColumn", Double.MIN_VALUE, Double.MIN_VALUE);
      testColumn(CONVERTERS, table, "doubleColumn", Double.MAX_VALUE, Double.MAX_VALUE);
      testColumn(CONVERTERS, table, "doubleColumn", 23.2d, 23.2d);
      testColumn(CONVERTERS, table, "floatColumn", Float.MIN_VALUE, Float.MIN_VALUE);
      testColumn(CONVERTERS, table, "floatColumn", Float.MAX_VALUE, Float.MAX_VALUE);
      testColumn(CONVERTERS, table, "yearColumn", new java.sql.Date(89, 1, 1), 1989);
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDecimalModeValues() {
    testDecimalModeValues(JdbcValueConverters.DecimalMode.DOUBLE);
    testDecimalModeValues(JdbcValueConverters.DecimalMode.STRING);
    testDecimalModeValues(JdbcValueConverters.DecimalMode.PRECISE);
  }

  private void testDecimalModeValues(JdbcValueConverters.DecimalMode mode) {
    SingleStoreValueConverters converters = new SingleStoreValueConverters(mode,
        TemporalPrecisionMode.CONNECT, CommonConnectorConfig.BinaryHandlingMode.BYTES,
        GeographyMode.GEOMETRY);
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      if (mode == JdbcValueConverters.DecimalMode.DOUBLE) {
        testColumn(converters, table, "decimalColumn", 100, (double) 100);
        testColumn(converters, table, "decColumn", 100.02, 100.02);
      } else if (mode == JdbcValueConverters.DecimalMode.PRECISE) {
        testColumn(converters, table, "decimalColumn", 100, BigDecimal.valueOf(100));
        testColumn(converters, table, "decColumn", 100.02, BigDecimal.valueOf(100.02));
      } else if (mode == JdbcValueConverters.DecimalMode.STRING) {
        testColumn(converters, table, "decimalColumn", 100, String.valueOf(100));
        testColumn(converters, table, "decColumn", 100.02, String.valueOf(100.02));
      }
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testGeographyGeometry() throws ParseException {
    String geographyValue = "POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))";
    String geographyPointValue = "POINT(1.50000003 1.50000000)";
    SingleStoreGeometry singleStoregeographyValue = SingleStoreGeometry.fromEkt(geographyValue);
    SingleStoreGeometry singleStoregeographyPointValue = SingleStoreGeometry.fromEkt(
        geographyPointValue);
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      // TODO: PLAT-6907
      // Struct convertedPolygon = (Struct) convertColumnValue(CONVERTERS, table, "geographyColumn", geographyValue);
      // assertArrayEquals((byte[]) convertedPolygon.get("wkb"), singleStoregeographyValue.getWkb());
      Struct convertedPoint = (Struct) convertColumnValue(CONVERTERS, table, "geographypointColumn",
          geographyPointValue);
      assertArrayEquals((byte[]) convertedPoint.get("wkb"),
          singleStoregeographyPointValue.getWkb());
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testGeographyString() throws ParseException {
    String geographyValue = "POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))";
    String geographyPointValue = "POINT(1.50000003 1.50000000)";
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();

      SingleStoreValueConverters converters = new SingleStoreValueConverters(
          JdbcValueConverters.DecimalMode.DOUBLE, TemporalPrecisionMode.CONNECT,
          CommonConnectorConfig.BinaryHandlingMode.BYTES, GeographyMode.STRING);
      String convertedPolygon = (String) convertColumnValue(converters, table, "geographyColumn",
          geographyValue);
      assertEquals("POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))", convertedPolygon);
      String convertedPoint = (String) convertColumnValue(converters, table, "geographypointColumn",
          geographyPointValue);
      assertEquals("POINT(1.50000003 1.50000000)", convertedPoint);
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testTimeAndDateValues() {
    testTimeAndDateValues(TemporalPrecisionMode.CONNECT);
    testTimeAndDateValues(TemporalPrecisionMode.ADAPTIVE);
    testTimeAndDateValues(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
  }

  private void testTimeAndDateValues(TemporalPrecisionMode mode) {
    SingleStoreValueConverters converters = new SingleStoreValueConverters(
        JdbcValueConverters.DecimalMode.DOUBLE, mode,
        CommonConnectorConfig.BinaryHandlingMode.BYTES, GeographyMode.GEOMETRY);
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      LocalDate date = LocalDate.of(1989, 3, 29);
      LocalDateTime dateTime = LocalDateTime.of(1989, 3, 29, 11, 22, 22);
      LocalDateTime dateTime6 = LocalDateTime.of(1989, 3, 29, 11, 22, 22, 123456000);
      Duration time = Duration.ofSeconds(LocalTime.of(22, 22, 22).getSecond());
      Duration time6 = Duration.ofNanos(LocalTime.of(22, 22, 22, 123456000).getNano());
      if (mode == TemporalPrecisionMode.ADAPTIVE
          || mode == TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS) {
        if (mode == TemporalPrecisionMode.ADAPTIVE) {
          testColumn(converters, table, "timeColumn", time, time.toMillis());
        } else {
          testColumn(converters, table, "timeColumn", time, time.toNanos() / 1_000);
        }
        testColumn(converters, table, "time6Column", time6, time6.toNanos() / 1_000);
        testColumn(converters, table, "dateColumn", java.sql.Date.valueOf(date),
            (int) date.atStartOfDay(ZoneId.of("UTC")).toEpochSecond() / 60 / 60 / 24); //epoch days
        testColumn(converters, table, "datetimeColumn", java.sql.Timestamp.valueOf(dateTime),
            Date.from(dateTime.atZone(ZoneId.of("UTC")).toInstant()).getTime()); // milliseconds
        testColumn(converters, table, "datetime6Column", java.sql.Timestamp.valueOf(dateTime6),
            Date.from(dateTime.atZone(ZoneId.of("UTC")).toInstant()).getTime() * 1_000
                + 123456);  //microseconds
        testColumn(converters, table, "timestampColumn", java.sql.Timestamp.valueOf(dateTime),
            Date.from(dateTime.atZone(ZoneId.of("UTC")).toInstant()).getTime());
        testColumn(converters, table, "timestamp6Column", java.sql.Timestamp.valueOf(dateTime6),
            Date.from(dateTime.atZone(ZoneId.of("UTC")).toInstant()).getTime() * 1_000 + 123456);
      } else {
        testColumn(converters, table, "timeColumn", time, new java.util.Date(time.toMillis()));
        testColumn(converters, table, "time6Column", time6, new java.util.Date(time6.toMillis()));
        testColumn(converters, table, "dateColumn", java.sql.Date.valueOf(date), new Date(
            date.atStartOfDay(ZoneId.of("UTC")).toEpochSecond() * 1_000)); //epoch seconds as date
        testColumn(converters, table, "datetimeColumn", java.sql.Timestamp.valueOf(dateTime),
            Date.from(dateTime.atZone(ZoneId.of("UTC")).toInstant())); // milliseconds
        testColumn(converters, table, "datetime6Column", java.sql.Timestamp.valueOf(dateTime6),
            new Date(Date.from(dateTime.atZone(ZoneId.of("UTC")).toInstant()).getTime()
                + 123));  //milliseconds
        testColumn(converters, table, "timestampColumn", java.sql.Timestamp.valueOf(dateTime),
            Date.from(dateTime.atZone(ZoneId.of("UTC")).toInstant()));
        testColumn(converters, table, "timestamp6Column", java.sql.Timestamp.valueOf(dateTime6),
            new Date(Date.from(dateTime.atZone(ZoneId.of("UTC")).toInstant()).getTime() + 123));
      }
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
      testColumn(CONVERTERS, table, "jsonColumn",
          "{ \"key1\": \"val1\", \"key2\": {\"key3\":\"val3\"} }",
          "{ \"key1\": \"val1\", \"key2\": {\"key3\":\"val3\"} }");
      testColumn(CONVERTERS, table, "enum_f", "val1", "val1");
      testColumn(CONVERTERS, table, "set_f", "val1", "val1");
      testColumn(CONVERTERS, table, "tinytextColumn", "val1", "val1");
      testColumn(CONVERTERS, table, "longtextColumn", "val1", "val1");
      testColumn(CONVERTERS, table, "textColumn", "val1", "val1");
      testColumn(CONVERTERS, table, "charColumn", "a", "a");
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
      byte[] blobValue = "blob value".getBytes();
      SingleStoreBlob blob = new SingleStoreBlob(blobValue);
      testColumn(CONVERTERS, table, "blobColumn", blob, ByteBuffer.wrap(blobValue));
      testColumn(CONVERTERS, table, "longblobColumn", blob, ByteBuffer.wrap(blobValue));
      testColumn(CONVERTERS, table, "mediumblobColumn", blob, ByteBuffer.wrap(blobValue));
      testColumn(CONVERTERS, table, "tinyblobColumn", blob, ByteBuffer.wrap(blobValue));
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testBinaryMode() {
    testBinaryMode(CommonConnectorConfig.BinaryHandlingMode.BYTES);
    testBinaryMode(CommonConnectorConfig.BinaryHandlingMode.BASE64);
    testBinaryMode(CommonConnectorConfig.BinaryHandlingMode.BASE64_URL_SAFE);
    testBinaryMode(CommonConnectorConfig.BinaryHandlingMode.HEX);
  }

  private void testBinaryMode(CommonConnectorConfig.BinaryHandlingMode mode) {
    SingleStoreValueConverters converters = new SingleStoreValueConverters(
        JdbcValueConverters.DecimalMode.DOUBLE, TemporalPrecisionMode.CONNECT, mode,
        GeographyMode.GEOMETRY);
    try (SingleStoreConnection conn = new SingleStoreConnection(defaultJdbcConnectionConfig())) {
      Tables tables = new Tables();
      conn.readSchema(tables, TEST_DATABASE, null, null, null, true);
      Table table = tables.forTable(TEST_DATABASE, null, "allTypesTable");
      assertThat(table).isNotNull();
      String value = "test value";
      String base64value = new String(
          Base64.getEncoder().encode(value.getBytes(StandardCharsets.UTF_8)));
      String base64urlValue = new String(
          Base64.getUrlEncoder().encode(value.getBytes(StandardCharsets.UTF_8)));
      if (mode == CommonConnectorConfig.BinaryHandlingMode.BYTES) {
        testColumn(converters, table, "varbinaryColumn", value,
            ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
        testColumn(converters, table, "blobColumn", value,
            ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
      } else if (mode == CommonConnectorConfig.BinaryHandlingMode.BASE64) {
        testColumn(converters, table, "varbinaryColumn", value, base64value);
        testColumn(converters, table, "blobColumn", value, base64value);
      } else if (mode == CommonConnectorConfig.BinaryHandlingMode.BASE64_URL_SAFE) {
        testColumn(converters, table, "varbinaryColumn", value, base64urlValue);
        testColumn(converters, table, "blobColumn", value, base64urlValue);
      } else if (mode == CommonConnectorConfig.BinaryHandlingMode.HEX) {
        testColumn(converters, table, "varbinaryColumn", value,
            HexConverter.convertToHexString(value.getBytes(StandardCharsets.UTF_8)));
        testColumn(converters, table, "blobColumn", value,
            HexConverter.convertToHexString(value.getBytes(StandardCharsets.UTF_8)));
      }
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  private static void testColumn(SingleStoreValueConverters converters, Table table, String name,
      Object valueToConvert, Object expectedConvertedValue) {
    assertEquals(expectedConvertedValue,
        convertColumnValue(converters, table, name, valueToConvert));
  }

  private static Object convertColumnValue(SingleStoreValueConverters converters, Table table,
      String name, Object valueToConvert) {
    Column column = table.columnWithName(name);
    Field field = new Field(column.name(), -1, converters.schemaBuilder(column).build());
    return converters.converter(column, field).convert(valueToConvert);
  }
}
