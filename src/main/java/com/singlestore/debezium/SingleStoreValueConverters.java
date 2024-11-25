package com.singlestore.debezium;

import com.singlestore.jdbc.SingleStoreBlob;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Json;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.Year;
import io.debezium.util.IoUtil;
import java.io.IOException;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.locationtech.jts.io.ParseException;

public class SingleStoreValueConverters extends JdbcValueConverters {

  private final GeographyMode geographyMode;

  /**
   * Create a new instance of JdbcValueConverters.
   * <p>
   *
   * @param decimalMode           how {@code DECIMAL} and {@code NUMERIC} values should be treated;
   *                              may be null if
   *                              {@link io.debezium.jdbc.JdbcValueConverters.DecimalMode#PRECISE}
   *                              is to be used
   * @param temporalPrecisionMode temporal precision mode based on
   *                              {@link io.debezium.jdbc.TemporalPrecisionMode}
   * @param binaryMode            how binary columns should be represented
   */
  public SingleStoreValueConverters(DecimalMode decimalMode,
      TemporalPrecisionMode temporalPrecisionMode,
      CommonConnectorConfig.BinaryHandlingMode binaryMode,
      GeographyMode geographyMode
  ) {
    super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryMode);
    this.geographyMode = geographyMode;
  }

  @Override
  public SchemaBuilder schemaBuilder(Column column) {
    String typeName = column.typeName().toUpperCase();
    switch (typeName) {
      case "JSON":
        return Json.builder();
      case "GEOGRAPHYPOINT":
      case "GEOGRAPHY":
        if (geographyMode == GeographyMode.GEOMETRY) {
          return io.debezium.data.geometry.Geometry.builder();
        } else {
          return SchemaBuilder.string();
        }
      case "ENUM":
      case "SET":
        return SchemaBuilder.string();
      case "TINYINT":
      case "TINYINT UNSIGNED":
        return SchemaBuilder.int16();
      case "YEAR":
        return Year.builder();
      case "MEDIUMINT":
      case "MEDIUMINT UNSIGNED":
        return SchemaBuilder.int32();
      case "FLOAT":
      case "FLOAT UNSIGNED":
        return SchemaBuilder.float32();
      case "TIME":
        if (adaptiveTimeMicrosecondsPrecisionMode) {
          return MicroTime.builder();
        }
        if (adaptiveTimePrecisionMode) {
          if (getTimePrecision(column) <= 10) {//TIME
            return SchemaBuilder.int64();
          }
          if (getTimePrecision(column) <= 17) {//TIME(6)
            return MicroTime.builder();
          }
        }
        return org.apache.kafka.connect.data.Time.builder();
      case "DATETIME":
      case "TIMESTAMP":
        if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
          if (getTimePrecision(column) <= 19) { //TIMESTAMP | DATETIME
            return Timestamp.builder();
          }
          if (getTimePrecision(column) <= 26) { //TIMESTAMP(6) | DATETIME(6)
            return MicroTimestamp.builder();
          }
        }
        return org.apache.kafka.connect.data.Timestamp.builder();
      case "TINYBLOB":
      case "LONGBLOB":
      case "MEDIUMBLOB":
      case "BLOB":
      case "BSON":
        return SchemaBuilder.bytes();
    }
    SchemaBuilder builder = super.schemaBuilder(column);
    logger.debug("JdbcValueConverters returned '{}' for column '{}'",
        builder != null ? builder.getClass().getName() : null, column.name());
    return builder;
  }

  @Override
  public ValueConverter converter(Column column, Field fieldDefn) {
    String typeName = column.typeName().toUpperCase();
    switch (typeName) {
      case "JSON":
        return (data) -> convertString(column, fieldDefn, data);
      case "GEOGRAPHYPOINT":
      case "GEOGRAPHY":
        if (geographyMode == GeographyMode.GEOMETRY) {
          return data -> convertGeometry(column, fieldDefn, data);
        } else {
          return data -> convertString(column, fieldDefn, data);
        }
      case "ENUM":
      case "SET":
        return data -> convertString(column, fieldDefn, data);
      case "TINYINT":
      case "TINYINT UNSIGNED":
        return (data -> convertSmallInt(column, fieldDefn, data));
      case "YEAR":
        return data -> convertYearToInt(column, fieldDefn, data);
      case "MEDIUMINT":
      case "MEDIUMINT UNSIGNED":
        return data -> convertInteger(column, fieldDefn, data);
      case "FLOAT":
      case "FLOAT UNSIGNED":
        return data -> convertReal(column, fieldDefn, data);
      case "TIME":
        return data -> convertTime(column, fieldDefn, data);
      case "DATETIME":
      case "TIMESTAMP":
        return data -> convertTimeStamp(column, fieldDefn, data);
      case "TINYBLOB":
      case "LONGBLOB":
      case "MEDIUMBLOB":
      case "BLOB":
      case "BSON":
        return data -> convertBlob(column, fieldDefn, data);
    }
    return super.converter(column, fieldDefn);
  }

  protected ByteOrder byteOrderOfBitType() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  /**
   * Converts SingleStoreBlob to byte array.
   *
   * @param column    the column definition describing the {@code data} value; never null
   * @param fieldDefn the field definition; never null
   * @param data      the data object to be converted into a {@link Date Kafka Connect date} type;
   *                  never null
   * @return the converted value, or null if the conversion could not be made and the column allows
   * nulls
   * @throws IllegalArgumentException if the value could not be converted but the column does not
   *                                  allow nulls
   */
  protected Object convertBlob(Column column, Field fieldDefn, Object data) {
    return convertValue(column, fieldDefn, data, 0, (r) -> {
      if (data instanceof SingleStoreBlob) {
        try {
          byte[] bytes = IoUtil.readBytes(((SingleStoreBlob) data).getBinaryStream());
          r.deliver(toByteBuffer(column, bytes));
        } catch (IOException | SQLException e) {
          throw new RuntimeException(e);
        }
      } else {
        r.deliver(super.convertBinary(column, fieldDefn, data, super.binaryMode));
      }
    });
  }

  /**
   * Converts java.sql.Timestamp returned from SingleStore for types: TIMESTAMP, DATETIME,
   * TIMESTAMP(6) and DATETIME(6).
   *
   * @param column    the column definition describing the {@code data} value; never null
   * @param fieldDefn the field definition; never null
   * @param data      the data object to be converted into a {@link Date Kafka Connect date} type;
   *                  never null
   * @return the converted value, or null if the conversion could not be made and the column allows
   * nulls
   */
  protected Object convertTimeStamp(Column column, Field fieldDefn, Object data) {
    if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
      if (getTimePrecision(column) <= 19) { //TIMESTAMP | DATETIME
        return convertTimestampToEpochMillis(column, fieldDefn, data);
      }
      if (getTimePrecision(column) <= 26) { //TIMESTAMP(6) | DATETIME(6)
        return convertTimestampToEpochMicros(column, fieldDefn, data);
      }
    }
    return convertTimestampToEpochMillisAsDate(column, fieldDefn, data);
  }

  /**
   * Converts returned value from SingleStore for types: TIME and TIME(6)
   * <p>
   *
   * @param column    the column definition describing the {@code data} value; never null
   * @param fieldDefn the field definition; never null
   * @param data      the data object to be converted into a {@link Date Kafka Connect date} type;
   *                  never null
   * @return the converted value, or null if the conversion could not be made and the column allows
   * nulls
   */
  @Override
  protected Object convertTime(Column column, Field fieldDefn, Object data) {
    if (adaptiveTimeMicrosecondsPrecisionMode) {
      return timeConverter(column, fieldDefn, convertTimeToMicros(data));
    }
    if (adaptiveTimePrecisionMode) {
      if (getTimePrecision(column) <= 10) { //TIME
        return timeConverter(column, fieldDefn, convertTimeToMilliseconds(data));
      }
      if (getTimePrecision(column) <= 17) { //TIME(6)
        return timeConverter(column, fieldDefn, convertTimeToMicros(data));
      }
    }

    return timeConverter(column, fieldDefn, convertTimeToMillisecondsAsDate(data));
  }

  private java.util.Date convertTimeToMillisecondsAsDate(Object data) {
    return new java.util.Date(convertTimeToMilliseconds(data));
  }

  private long convertTimeToMilliseconds(Object data) {
    return (convertTimeToMicros(data) / 1000);
  }

  private long convertTimeToMicros(Object data) {
    if (data instanceof Duration) {
      return ((Duration) data).toNanos() / 1000;
    } else if (data instanceof java.sql.Timestamp) {
      java.sql.Timestamp time = (java.sql.Timestamp) data;
      LocalDateTime local = time.toLocalDateTime();
      long seconds = local.toEpochSecond(ZoneOffset.UTC);
      int nanos = local.getNano();

      return seconds * 1000000 + nanos / 1000;
    } else {
      throw new IllegalArgumentException();
    }
  }

  private Object timeConverter(Column column, Field fieldDefn, Object time) {
    // epoch is the fallback value
    return convertValue(column, fieldDefn, time, 0L, (r) -> {
      try {
        r.deliver(time);
      } catch (IllegalArgumentException ignored) {
      }
    });
  }

  /**
   * Converts a value object for {@code YEAR}
   *
   * @param column    the column definition describing the {@code data} value; never null
   * @param fieldDefn the field definition; never null
   * @param data      the data object to be converted into a year literal integer value; never null
   * @return the converted value, or null if the conversion could not be made and the column allows
   * nulls
   */
  @SuppressWarnings("deprecation")
  protected Object convertYearToInt(Column column, Field fieldDefn, Object data) {
    return convertValue(column, fieldDefn, data, 0, (r) -> {
      Object mutData = data;
      if (data instanceof java.sql.Date) {
        r.deliver(((java.sql.Date) data).getYear() + 1900);
      } else if (data instanceof String) {
        mutData = Integer.parseInt((String) data);
      }
      if (mutData instanceof Number) {
        r.deliver(java.time.Year.of(((Number) mutData).intValue()).get(ChronoField.YEAR));
      }
    });
  }

  /**
   * Convert the value representing a GEOMETRY {@code byte[]} value to a Geometry value used in a
   * {@link SourceRecord}.
   *
   * @param column    the column in which the value appears
   * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never
   *                  null
   * @param data      the data; may be null
   * @return the converted value, or null if the conversion could not be made and the column allows
   * nulls
   * @throws IllegalArgumentException if the value could not be converted but the column does not
   *                                  allow nulls
   */
  protected Object convertGeometry(Column column, Field fieldDefn, Object data)
      throws IllegalArgumentException {
    SingleStoreGeometry empty = SingleStoreGeometry.createEmpty();
    return convertValue(column, fieldDefn, data,
        io.debezium.data.geometry.Geometry.createValue(fieldDefn.schema(), empty.getWkb(),
            empty.getSrid()), (r) -> {
          if (data instanceof String) {
            SingleStoreGeometry geometry;
            try {
              geometry = SingleStoreGeometry.fromEkt((String) data);
            } catch (ParseException e) {
              throw new IllegalArgumentException(e);
            }
            r.deliver(io.debezium.data.geometry.Geometry.createValue(fieldDefn.schema(),
                geometry.getWkb(), geometry.getSrid()));
          }
        });
  }

  public enum GeographyMode {
    GEOMETRY,
    STRING
  }

  public enum VectorMode {
    STRING,
    BINARY,
    ARRAY
  }
}
