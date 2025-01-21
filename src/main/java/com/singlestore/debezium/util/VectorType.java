package com.singlestore.debezium.util;

import java.util.Arrays;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class VectorType {

  private final Integer lengthInBytes;
  private final SchemaBuilder schema;
  private ElementType elementType;

  public VectorType(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Type name is null");
    }

    if (value.length() < 13) {
      throw new IllegalArgumentException(
          String.format("Too short type name for VECTOR: %s", value));
    }

    // Skip "VECTOR(" and ")"
    // "VECTOR(3, I8)" -> "3, I8"
    value = value.substring(7, value.length() - 1);

    // Split into length and element type and trim
    // "3, I8" -> ["3", "I8"]
    String[] parts = Arrays.stream(value.split(","))
        .map(String::trim)
        .toArray(String[]::new);
    if (parts.length != 2) {
      throw new IllegalArgumentException(String.format("Invalid VECTOR type: %s", value));
    }

    String elementTypeStr = parts[1];
    // Find corresponding enum value
    for (ElementType option : ElementType.values()) {
      if (option.value.equalsIgnoreCase(elementTypeStr)) {
        this.elementType = option;
      }
    }
    if (this.elementType == null) {
      throw new IllegalArgumentException(
          String.format("Invalid VECTOR element type: %s", elementTypeStr));
    }

    this.schema = SchemaBuilder.array(this.elementType.schema);
    this.lengthInBytes = this.elementType.lengthInBytes * Integer.parseInt(parts[0]);
  }

  public SchemaBuilder getSchema() {
    return schema;
  }

  public ElementType getElementType() {
    return elementType;
  }

  public Integer getLengthInBytes() {
    return lengthInBytes;
  }

  public enum ElementType {
    INT8("I8", SchemaBuilder.INT8_SCHEMA, 1),
    INT16("I16", SchemaBuilder.INT16_SCHEMA, 2),
    INT32("I32", SchemaBuilder.INT32_SCHEMA, 4),
    INT64("I64", SchemaBuilder.INT64_SCHEMA, 8),
    FLOAT32("F32", SchemaBuilder.FLOAT32_SCHEMA, 4),
    FLOAT64("F64", SchemaBuilder.FLOAT64_SCHEMA, 8);

    private final String value;
    private final Schema schema;
    private final Integer lengthInBytes;

    ElementType(String value, Schema schema, Integer lengthInBytes) {
      this.value = value;
      this.schema = schema;
      this.lengthInBytes = lengthInBytes;
    }

    public Integer getLengthInBytes() {
      return lengthInBytes;
    }
  }
}