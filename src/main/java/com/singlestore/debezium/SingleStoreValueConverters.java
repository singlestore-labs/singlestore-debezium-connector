package com.singlestore.debezium;

import com.singlestore.debezium.util.Utils;
import com.singlestore.debezium.util.VectorType;
import com.singlestore.debezium.util.VectorType.ElementType;
import com.singlestore.jdbc.SingleStoreBlob;
import com.singlestore.jdbc.type.Vector;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.locationtech.jts.io.ParseException;

public class SingleStoreValueConverters extends JdbcValueConverters {

  private final GeographyMode geographyMode;
  private final VectorMode vectorMode;

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
      GeographyMode geographyMode,
      VectorMode vectorMode
  ) {
    super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryMode);
    this.geographyMode = geographyMode;
    this.vectorMode = vectorMode;
  }

  @Override
  public SchemaBuilder schemaBuilder(Column column) {
    String typeName = Utils.getOriginalTypeName(column.typeName());
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
      case "VECTOR":
        switch (vectorMode) {
          case STRING:
            return SchemaBuilder.string();
          case BINARY:
            return binaryMode.getSchema();
          case ARRAY:
            return new VectorType(column.typeName()).getSchema();
          default:
            throw new IllegalArgumentException(
                String.format("Unknown vector.handling.mode: %s", vectorMode));
        }
    }
    SchemaBuilder builder = super.schemaBuilder(column);
    logger.debug("JdbcValueConverters returned '{}' for column '{}'",
        builder != null ? builder.getClass().getName() : null, column.name());
    return builder;
  }

  @Override
  public ValueConverter converter(Column column, Field fieldDefn) {
    String typeName = Utils.getOriginalTypeName(column.typeName());
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
      case "VECTOR":
        return data -> convertVector(column, fieldDefn, data);
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
          r.deliver(super.convertBinary(column, fieldDefn, bytes, super.binaryMode));
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

  /**
   * Reverses each element of an array from little endian to big endian
   **/
  private void reverseToBigEndian(byte[] arr, int bytesPerElement) {
    assert arr.length % bytesPerElement == 0;

    for (int i = 0; i < arr.length; i += bytesPerElement) {
      int start = i;
      int end = i + bytesPerElement - 1;
      while (start < end) {
        // Swap elements at start and end indices
        byte temp = arr[start];
        arr[start] = arr[end];
        arr[end] = temp;

        // Move the pointers
        start++;
        end--;
      }
    }
  }

  /**
   * TODO: fix
   * Converts SingleStoreBlob to array.
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
  protected Object convertVector(Column column, Field fieldDefn, Object data) {
    return convertValue(column, fieldDefn, data, 0, (r) -> {
      if (data instanceof String) {
        // Data is String only in the case when we deal with default values.
        // Here we should have a JSON representation of the default column value.

        String dataString = (String) data;
        switch (this.vectorMode) {
          case STRING:
            r.deliver(dataString);
            break;
          case BINARY:
            // delete [] brackets (the first and the last symbol)
            // "[1, 2, 3]" -> "1, 2, 3"
            dataString = dataString.substring(1, dataString.length() - 1);

            // split string into separate elements and trim each of them
            // "1, 2, 3" -> ["1", "2", "3"]
            List<String> values = Arrays.stream(dataString.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

            VectorType vectorType = new VectorType(column.typeName());
            ByteBuffer resBuffer = ByteBuffer.allocate(vectorType.getLengthInBytes());
            ElementType type = vectorType.getElementType();
            switch (type) {
              case INT8:
                values.stream()
                    .map(Byte::parseByte)
                    .forEach(resBuffer::put);
                break;
              case INT16:
                values.stream()
                    .map(Short::parseShort)
                    .forEach(resBuffer::putShort);
                break;
              case INT32:
                values.stream()
                    .map(Integer::parseInt)
                    .forEach(resBuffer::putInt);
                break;
              case INT64:
                values.stream()
                    .map(Long::parseLong)
                    .forEach(resBuffer::putLong);
                break;
              case FLOAT32:
                values.stream()
                    .map(Float::parseFloat)
                    .forEach(resBuffer::putFloat);
                break;
              case FLOAT64:
                values.stream()
                    .map(Double::parseDouble)
                    .forEach(resBuffer::putDouble);
                break;
              default:
                throw new IllegalArgumentException(
                    String.format("Unknown vector element type: %s", type));
            }

            byte[] res = resBuffer.array();
            reverseToBigEndian(res, vectorType.getElementType().getLengthInBytes());

            r.deliver(super.convertBinary(column, fieldDefn, res, super.binaryMode));
            break;
          case ARRAY:
            // delete [] brackets (the first and the last symbol)
            // "[1, 2, 3]" -> "1, 2, 3"
            dataString = dataString.substring(1, dataString.length() - 1);

            // split string into separate elements and trim each of them
            // "1, 2, 3" -> ["1", "2", "3"]
            List<String> elements = Arrays.stream(dataString.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

            ElementType elementType = new VectorType(column.typeExpression()).getElementType();
            switch (elementType) {
              case INT8:
                r.deliver(elements.stream()
                    .map(Byte::parseByte)
                    .collect(Collectors.toList()));
                return;
              case INT16:
                r.deliver(elements.stream()
                    .map(Short::parseShort)
                    .collect(Collectors.toList()));
                return;
              case INT32:
                r.deliver(elements.stream()
                    .map(Integer::parseInt)
                    .collect(Collectors.toList()));
                return;
              case INT64:
                r.deliver(elements.stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toList()));
                return;
              case FLOAT32:
                r.deliver(elements.stream()
                    .map(Float::parseFloat)
                    .collect(Collectors.toList()));
                return;
              case FLOAT64:
                r.deliver(elements.stream()
                    .map(Double::parseDouble)
                    .collect(Collectors.toList()));
                return;
              default:
                throw new IllegalArgumentException(
                    String.format("Unknown vector element type: %s", elementType));
            }
          default:
            throw new IllegalArgumentException(
                String.format("Unknown vector mode: %s", vectorMode));
        }
      } else if (data instanceof byte[]) {
        // Data is byte[] only when vector.mode is BINARY.
        // This is because we retrieve it using rs.getBytes instead of rs.getObject.
        // Such handling allows to avoid redundant conversions to Vector.

        r.deliver(super.convertBinary(column, fieldDefn, data, super.binaryMode));
      } else if (data instanceof Vector) {
        Vector v = (Vector) data;
        switch (vectorMode) {
          case ARRAY:
            switch (new VectorType(column.typeExpression()).getElementType()) {
              case INT8:
                byte[] byteArray = v.toByteArray();
                r.deliver(IntStream.range(0, byteArray.length)
                    .mapToObj(i -> byteArray[i]) // Convert to Byte
                    .collect(Collectors.toList()));
                return;
              case INT16:
                short[] shortArray = v.toShortArray();
                r.deliver(IntStream.range(0, shortArray.length)
                    .mapToObj(i -> shortArray[i]) // Convert to Short
                    .collect(Collectors.toList()));
                return;
              case INT32:
                int[] intArray = v.toIntArray();
                r.deliver(IntStream.of(intArray)
                    .boxed()
                    .collect(Collectors.toList()));
                return;
              case INT64:
                long[] longArray = v.toLongArray();
                r.deliver(LongStream.of(longArray)
                    .boxed()
                    .collect(Collectors.toList()));
                return;
              case FLOAT32:
                float[] floatArray = v.toFloatArray();
                r.deliver(IntStream.range(0, floatArray.length)
                    .mapToObj(i -> floatArray[i]) // Convert to Float
                    .collect(Collectors.toList()));
                return;
              case FLOAT64:
                double[] doubleArray = v.toDoubleArray();
                r.deliver(DoubleStream.of(doubleArray).boxed()
                    .collect(Collectors.toList()));
                return;
            }
          case BINARY:
            // If vector.mode is BINARY then data should be byte[].
            // This is because we retrieve it using rs.getBytes instead of rs.getObject.
            // This code path should not be reachable.
            throw new IllegalStateException("This code path should not be reachable.");
          case STRING:
            r.deliver(v.stringValue());
            return;
          default:
            throw new IllegalArgumentException(
                String.format("Unknown vector mode: %s", vectorMode));
        }
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
