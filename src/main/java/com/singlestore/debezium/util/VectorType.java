package com.singlestore.debezium.util;

import org.apache.kafka.connect.data.SchemaBuilder;

public enum VectorType {
  INT8("I8", SchemaBuilder.array(SchemaBuilder.INT8_SCHEMA)),
  INT16("I16", SchemaBuilder.array(SchemaBuilder.INT16_SCHEMA)),
  INT32("I32", SchemaBuilder.array(SchemaBuilder.INT32_SCHEMA)),
  INT64("I64", SchemaBuilder.array(SchemaBuilder.INT64_SCHEMA)),
  FLOAT32("F32", SchemaBuilder.array(SchemaBuilder.FLOAT32_SCHEMA)),
  FLOAT64("F64", SchemaBuilder.array(SchemaBuilder.FLOAT64_SCHEMA));

  private final String value;
  private final SchemaBuilder schema;

  VectorType(String value, SchemaBuilder schema) {
    this.value = value;
    this.schema = schema;
  }

  public static VectorType parse(String value) {
    if (value == null) {
      return null;
    }

    // Take part after the comma and skip space
    // "VECTOR(3, I8)" -> "I8)"
    int commaPosition = value.lastIndexOf(", ");
    if (commaPosition == -1) {
      return null;
    }
    value = value.substring(commaPosition + 2);

    // Delete bracket at the end
    // "I8)" -> "I8"
    if (value.isEmpty()) {
      return null;
    }
    value = value.substring(0, value.length() - 1);

    // Find corresponding enum value
    for (VectorType option : VectorType.values()) {
      if (option.getValue().equalsIgnoreCase(value)) {
        return option;
      }
    }

    return null;
  }

  public String getValue() {
    return value;
  }

  public SchemaBuilder getSchema() {
    return schema;
  }
}
