package com.singlestore.debezium;

import com.singlestore.jdbc.SingleStoreBlob;
import io.debezium.data.Bits;
import java.sql.Types;
import java.util.BitSet;
import org.locationtech.jts.io.ParseException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Json;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.*;
import io.debezium.util.IoUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;

public class SingleStoreValueConverters extends JdbcValueConverters {

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
      CommonConnectorConfig.BinaryHandlingMode binaryMode) {
    super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryMode);
  }

  @Override
  public SchemaBuilder schemaBuilder(Column column) {
    String typeName = column.typeName().toUpperCase();
    switch (typeName) {
      case "JSON":
        return Json.builder();
      case "GEOGRAPHYPOINT":
      case "GEOGRAPHY":
        return io.debezium.data.geometry.Geometry.builder();
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
            return Time.builder();
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
        return SchemaBuilder.bytes();
      case "BIT":
        return Bits.builder(64);
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
        return data -> convertGeometry(column, fieldDefn, data);
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
        return data -> convertBlob(column, fieldDefn, data);
    }
    return super.converter(column, fieldDefn);
  }

  @Override
  protected ValueConverter convertBits(Column column, Field fieldDefn) {
    int numBits = column.length();
    int numBytes = numBits / Byte.SIZE + (numBits % Byte.SIZE == 0 ? 0 : 1);
    return (data) -> convertBits(column, fieldDefn, data, numBytes);
  }

  /**
   * Converts a value object for an expected JDBC type of {@link Types#BIT} of length 2+.
   *
   * @param column    the column definition describing the {@code data} value; never null
   * @param fieldDefn the field definition; never null
   * @param data      the data object to be converted into a {@link Date Kafka Connect date} type;
   *                  never null
   * @param numBytes  the number of bytes that should be included in the resulting byte[]
   * @return the converted value, or null if the conversion could not be made and the column allows
   * nulls
   * @throws IllegalArgumentException if the value could not be converted but the column does not
   *                                  allow nulls
   */
  @Override
  protected Object convertBits(Column column, Field fieldDefn, Object data, int numBytes) {
    return convertValue(column, fieldDefn, data, new byte[0], (r) -> {
      byte[] bytes;
      if (data instanceof byte[]) {
        bytes = (byte[]) data;
      } else if (data instanceof BitSet) {
        bytes = ((BitSet) data).toByteArray();

        // BitSet in SingleStore JDBC driver returns bytes in wrong order
        int i = 0;
        int j = bytes.length - 1;
        byte tmp;
        while (j > i) {
          tmp = bytes[j];
          bytes[j] = bytes[i];
          bytes[i] = tmp;
          ++i;
          --j;
        }
      } else {
        throw new IllegalArgumentException("Invalid data type of BIT column");
      }

      r.deliver(padBigEndian(numBytes, bytes));
    });
  }

  protected byte[] padBigEndian(int numBytes, byte[] data) {
    if (data.length < numBytes) {
      byte[] padded = new byte[numBytes];
      System.arraycopy(data, 0, padded, numBytes - data.length, data.length);
      return padded;
    }
    return data;
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
      return convertTimeToMicrosPastMidnight(column, fieldDefn, data);
    }
    if (adaptiveTimePrecisionMode) {
      if (getTimePrecision(column) <= 10) { //TIME
        return convertTimeToMillisPastMidnight(column, fieldDefn, data);
      }
      if (getTimePrecision(column) <= 17) { //TIME(6)
        return convertTimeToMicrosPastMidnight(column, fieldDefn, data);
      }
    }
    return convertTimeToMillisPastMidnightAsDate(column, fieldDefn, data);
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
}
