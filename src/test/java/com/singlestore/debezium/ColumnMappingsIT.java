package com.singlestore.debezium;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import io.debezium.config.Configuration;
import java.sql.SQLException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

public class ColumnMappingsIT extends IntegrationTestBase {

  @Test
  public void testHashMask() throws SQLException, InterruptedException {
    try (SingleStoreDBConnection conn = new SingleStoreDBConnection(
        defaultJdbcConnectionConfigWithTable("song"))) {
      Configuration config = defaultJdbcConfigWithTable("song");
      config = config.edit()
          .with("column.mask.hash.SHA-256.with.salt.salt123", TEST_DATABASE + ".song.author")
          .with("column.mask.with.10.chars", TEST_DATABASE + ".song.name").build();

      start(SingleStoreDBConnector.class, config);
      assertConnectorIsRunning();

      try {
        conn.execute("INSERT INTO `song` VALUES ('test1', 'test1')");
        conn.execute("INSERT INTO `song` VALUES ('test1', 'test2')");
        conn.execute("INSERT INTO `song` VALUES ('test1', 'test3')");

        List<SourceRecord> records = consumeRecordsByTopic(3).allRecordsInOrder();

        assertEquals(3, records.size());
        for (int i = 0; i < records.size(); i++) {
          SourceRecord record = records.get(i);
          Struct value = (Struct) ((Struct) record.value()).get("after");
          assertNotEquals("test" + (i + 1), value.get("author"));
          assertEquals("**********", value.get("name"));
        }
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void testTruncate() throws SQLException, InterruptedException {
    try (SingleStoreDBConnection conn = new SingleStoreDBConnection(
        defaultJdbcConnectionConfigWithTable("song"))) {
      Configuration config = defaultJdbcConfigWithTable("song");
      config = config.edit().with("column.truncate.to.10.chars",
          TEST_DATABASE + ".song.author," + TEST_DATABASE + ".song.name").build();

      start(SingleStoreDBConnector.class, config);
      assertConnectorIsRunning();

      try {
        conn.execute("INSERT INTO `song` VALUES ('12345678901234567890', '12345678901234567890')");
        conn.execute("INSERT INTO `song` VALUES ('12345678901234567890', '12345678901234567890')");
        conn.execute("INSERT INTO `song` VALUES ('12345678901234567890', '12345678901234567890')");

        List<SourceRecord> records = consumeRecordsByTopic(3).allRecordsInOrder();

        assertEquals(3, records.size());
        for (SourceRecord record : records) {
          Struct value = (Struct) ((Struct) record.value()).get("after");
          assertEquals("1234567890", value.get("author"));
          assertEquals("1234567890", value.get("name"));
        }
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void testColumnPropagate() throws SQLException, InterruptedException {
    try (SingleStoreDBConnection conn = new SingleStoreDBConnection(
        defaultJdbcConnectionConfigWithTable("person"))) {
      Configuration config = defaultJdbcConfigWithTable("person");
      config = config.edit().with(SingleStoreDBConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE, ".*")
          .build();

      start(SingleStoreDBConnector.class, config);
      assertConnectorIsRunning();

      try {
        conn.execute("INSERT INTO `person` (`name`, `birthdate`, `age`, `salary`, `bitStr`) "
            + "VALUES ('Mike', '1999-01-01', 10, 100, 'a')");
        List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertEquals(1, records.size());
        SourceRecord record = records.get(0);
        Schema afterSchema = record.valueSchema().field("after").schema();
        assertThat(afterSchema.field("name").schema().parameters()
            .get("__debezium.source.column.type")).isEqualTo("VARCHAR");
        assertThat(afterSchema.field("name").schema().parameters()
            .get("__debezium.source.column.length")).isEqualTo("255");
        assertThat(afterSchema.field("name").schema().parameters()
            .get("__debezium.source.column.name")).isEqualTo("name");

        assertThat(afterSchema.field("birthdate").schema().parameters()
            .get("__debezium.source.column.type")).isEqualTo("DATE");
        assertThat(afterSchema.field("birthdate").schema().parameters()
            .get("__debezium.source.column.length")).isEqualTo("10");
        assertThat(afterSchema.field("birthdate").schema().parameters()
            .get("__debezium.source.column.name")).isEqualTo("birthdate");

        assertThat(afterSchema.field("age").schema().parameters()
            .get("__debezium.source.column.type")).isEqualTo("INT");
        assertThat(afterSchema.field("age").schema().parameters()
            .get("__debezium.source.column.length")).isEqualTo("10");
        assertThat(afterSchema.field("age").schema().parameters()
            .get("__debezium.source.column.scale")).isEqualTo("0");
        assertThat(afterSchema.field("age").schema().parameters()
            .get("__debezium.source.column.name")).isEqualTo("age");

        assertThat(afterSchema.field("salary").schema().parameters()
            .get("__debezium.source.column.type")).isEqualTo("DECIMAL");
        assertThat(afterSchema.field("salary").schema().parameters()
            .get("__debezium.source.column.length")).isEqualTo("5");
        assertThat(afterSchema.field("salary").schema().parameters()
            .get("__debezium.source.column.scale")).isEqualTo("2");
        assertThat(afterSchema.field("salary").schema().parameters()
            .get("__debezium.source.column.name")).isEqualTo("salary");
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void testDataTypePropagate() throws SQLException, InterruptedException {
    try (SingleStoreDBConnection conn = new SingleStoreDBConnection(
        defaultJdbcConnectionConfigWithTable("person"))) {
      Configuration config = defaultJdbcConfigWithTable("person");
      config = config.edit().with(SingleStoreDBConnectorConfig.PROPAGATE_DATATYPE_SOURCE_TYPE,
          ".+\\.VARCHAR,.+\\.DECIMAL").build();

      start(SingleStoreDBConnector.class, config);
      assertConnectorIsRunning();

      try {
        conn.execute("INSERT INTO `person` (`name`, `birthdate`, `age`, `salary`, `bitStr`) "
            + "VALUES ('Mike', '1999-01-01', 10, 100, 'a')");
        List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertEquals(1, records.size());
        SourceRecord record = records.get(0);
        Schema afterSchema = record.valueSchema().field("after").schema();
        assertThat(afterSchema.field("name").schema().parameters()
            .get("__debezium.source.column.type")).isEqualTo("VARCHAR");
        assertThat(afterSchema.field("name").schema().parameters()
            .get("__debezium.source.column.length")).isEqualTo("255");
        assertThat(afterSchema.field("name").schema().parameters()
            .get("__debezium.source.column.name")).isEqualTo("name");

        assertThat(afterSchema.field("birthdate").schema().parameters()).isNull();
        assertThat(afterSchema.field("age").schema().parameters()).isNull();

        assertThat(afterSchema.field("salary").schema().parameters()
            .get("__debezium.source.column.type")).isEqualTo("DECIMAL");
        assertThat(afterSchema.field("salary").schema().parameters()
            .get("__debezium.source.column.length")).isEqualTo("5");
        assertThat(afterSchema.field("salary").schema().parameters()
            .get("__debezium.source.column.scale")).isEqualTo("2");
        assertThat(afterSchema.field("salary").schema().parameters()
            .get("__debezium.source.column.name")).isEqualTo("salary");
      } finally {
        stopConnector();
      }
    }
  }
}
