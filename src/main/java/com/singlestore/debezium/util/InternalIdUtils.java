package com.singlestore.debezium.util;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class InternalIdUtils {

  public static final String INTERNAL_ID = "internalId";

  public static Schema getKeySchema(Table table, TableSchema schema) {
    if (useInternalIdAsKey(table)) {
      return SchemaBuilder.struct().field(INTERNAL_ID, Schema.INT64_SCHEMA).build();
    } else {
      return schema.keySchema();
    }
  }

  private static boolean useInternalIdAsKey(Table table) {
    return table.primaryKeyColumnNames().isEmpty();
  }

  public static Struct generateKey(Table table, TableSchema tableSchema, Object[] values,
      Long internalId) {
    if (useInternalIdAsKey(table)) {
      return keyFromInternalId(internalId);
    } else {
      return tableSchema.keyFromColumnData(values);
    }
  }

  private static Struct keyFromInternalId(Long internalId) {
    Struct result = new Struct(
        SchemaBuilder.struct().field(INTERNAL_ID, Schema.INT64_SCHEMA).build());
    result.put(INTERNAL_ID, internalId);
    return result;
  }

  public static Schema addInternalId(Schema s) {
    SchemaBuilder res = SchemaBuilder.struct();
    for (Field f : s.fields()) {
      res.field(f.name(), f.schema());
    }
    res.field(INTERNAL_ID, Schema.INT64_SCHEMA);

    return res.optional().build();
  }

  public static List<Column> addInternalId(List<Column> columns) {
    List<Column> result = new ArrayList<>(columns);
    result.add(Column.editor()
        .name(INTERNAL_ID)
        .position(result.size() + 1)
        .type("INT64")
        .autoIncremented(false)
        .generated(false)
        .optional(false)
        .jdbcType(-5)
        .nativeType(-1)
        .length(19)
        .scale(0)
        .create());

    return result;
  }

  private InternalIdUtils() {
  }
}
