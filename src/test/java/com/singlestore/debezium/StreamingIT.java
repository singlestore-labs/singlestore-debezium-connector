package com.singlestore.debezium;

import static io.debezium.embedded.EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import ch.qos.logback.classic.Logger;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.EmbeddedEngine.CompletionResult;
import io.debezium.engine.DebeziumEngine;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;
import org.slf4j.LoggerFactory;

public class StreamingIT extends IntegrationTestBase {

  @Test
  public void canReadAllTypes() throws SQLException, ParseException, InterruptedException {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("allTypesTable"))) {
      Configuration config = defaultJdbcConfigWithTable("allTypesTable");
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForStreamingToStart();
      try {
        conn.execute("INSERT INTO `allTypesTable` VALUES (\n" + "TRUE, " + // boolColumn
            "TRUE, " + // booleanColumn
            "'abcdefgh', " + // bitColumn
            "-128, " + // tinyintColumn
            "-8388608, " + // mediumintColumn
            "-32768, " + // smallintColumn
            "-2147483648, " + // intColumn
            "-2147483648, " + // integerColumn
            "-9223372036854775808, " + // bigintColumn
            "-100.01, " + // floatColumn
            "-1000.01, " + // doubleColumn
            "-1000.01, " + // realColumn
            "'1000-01-01', " + // dateColumn
            // Negative time returns incorrect result
            // It is converted to 24h - time during reading of the result
            "'0:00:00', " + // timeColumn
            "'0:00:00.000000', " + // time6Column
            "'1000-01-01 00:00:00', " + // datetimeColumn
            "'1000-01-01 00:00:00.000000', " + // datetime6Column
            "'1970-01-01 00:00:01', " + // timestampColumn
            "'1970-01-01 00:00:01.000000', " + // timestamp6Column
            "1901, " + // yearColumn
            "12345678901234567890123456789012345.123456789012345678901234567891, " +
            // decimalColumn
            "1234567890, " + // decColumn
            "1234567890, " + // fixedColumn
            "1234567890, " + // numericColumn
            "'a', " + // charColumn
            "'abc', " + // mediumtextColumn
            "'a', " + // binaryColumn
            "'abc', " + // varcharColumn
            "'abc', " + // varbinaryColumn
            "'abc', " + // longtextColumn
            "'abc', " + // textColumn
            "'abc', " + // tinytextColumn
            "'abc', " + // longblobColumn
            "'abc', " + // mediumblobColumn
            "'abc', " + // blobColumn
            "'abc', " + // tinyblobColumn
            "'{}', " + // jsonColumn
            "'val1', " + // enum_f
            "'v1', " + // set_f
            "'POLYGON((1 1,2 1,2 2, 1 2, 1 1))', " +
            // geographyColumn TODO: PLAT-6907 test GEOGRAPHY datatype
            "'POINT(1.50000003 1.50000000)')" // geographypointColumn
        );

        List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertEquals(1, records.size());

        SourceRecord record = records.get(0);
        Struct value = (Struct) record.value();
        Struct after = (Struct) value.get("after");
        Struct source = (Struct) value.get("source");

        assertEquals(true, record.sourceOffset().get("snapshot_completed"));
        assertEquals("false", source.get("snapshot"));

        // TODO: PLAT-6909 handle BOOL columns as boolean
        assertEquals((short) 1, after.get("boolColumn"));
        assertEquals((short) 1, after.get("booleanColumn"));
        // TODO: PLAT-6910 BIT type is returned in reversed order
        assertArrayEquals("hgfedcba".getBytes(), (byte[]) after.get("bitColumn"));
        assertEquals((short) -128, after.get("tinyintColumn"));
        assertEquals(-8388608, after.get("mediumintColumn"));
        assertEquals((short) -32768, after.get("smallintColumn"));
        assertEquals(-2147483648, after.get("intColumn"));
        assertEquals(-2147483648, after.get("integerColumn"));
        assertEquals(-9223372036854775808L, after.get("bigintColumn"));
        assertEquals((float) -100.01, after.get("floatColumn"));
        assertEquals(-1000.01, after.get("doubleColumn"));
        assertEquals(-1000.01, after.get("realColumn"));
        assertEquals(-354285, after.get("dateColumn"));
        assertEquals(0, after.get("timeColumn"));
        assertEquals((long) 0, after.get("time6Column"));
        assertEquals(-30610224000000L, after.get("datetimeColumn"));
        assertEquals(-30610224000000000L, after.get("datetime6Column"));
        assertEquals((long) 1000, after.get("timestampColumn"));
        assertEquals((long) 1000000, after.get("timestamp6Column"));
        assertEquals(1901, after.get("yearColumn"));
        assertEquals(
            new BigDecimal("12345678901234567890123456789012345.123456789012345678901234567891"),
            after.get("decimalColumn"));
        assertEquals(new BigDecimal("1234567890"), after.get("decColumn"));
        assertEquals(new BigDecimal("1234567890"), after.get("fixedColumn"));
        assertEquals(new BigDecimal("1234567890"), after.get("numericColumn"));
        assertEquals("a", after.get("charColumn"));
        assertEquals("abc", after.get("mediumtextColumn"));
        assertEquals(ByteBuffer.wrap("a".getBytes()), after.get("binaryColumn"));
        assertEquals("abc", after.get("varcharColumn"));
        assertEquals(ByteBuffer.wrap("abc".getBytes()), after.get("varbinaryColumn"));
        assertEquals("abc", after.get("longtextColumn"));
        assertEquals("abc", after.get("textColumn"));
        assertEquals("abc", after.get("tinytextColumn"));
        assertEquals(ByteBuffer.wrap("abc".getBytes()), after.get("longblobColumn"));
        assertEquals(ByteBuffer.wrap("abc".getBytes()), after.get("mediumblobColumn"));
        assertEquals(ByteBuffer.wrap("abc".getBytes()), after.get("blobColumn"));
        assertEquals(ByteBuffer.wrap("abc".getBytes()), after.get("tinyblobColumn"));
        assertEquals("{}", after.get("jsonColumn"));
        assertEquals("val1", after.get("enum_f"));
        assertEquals("v1", after.get("set_f"));
        String geographyValue = "POLYGON((1 1,2 1,2 2, 1 2, 1 1))";
        SingleStoreGeometry singleStoregeographyValue = SingleStoreGeometry.fromEkt(
            geographyValue);
        assertArrayEquals((byte[]) ((Struct) after.get("geographyColumn")).get("wkb"),
            singleStoregeographyValue.getWkb());
        String geographyPointValue = "POINT(1.50000003 1.50000000)";
        SingleStoreGeometry singleStoregeographyPointValue = SingleStoreGeometry.fromEkt(
            geographyPointValue);
        assertArrayEquals((byte[]) ((Struct) after.get("geographypointColumn")).get("wkb"),
            singleStoregeographyPointValue.getWkb());
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void populatesSourceInfo() throws SQLException, InterruptedException {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("purchased"))) {
      Configuration config = defaultJdbcConfigWithTable("purchased");
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForStreamingToStart();
      try {
        conn.execute("INSERT INTO `purchased` VALUES ('archie', 1, NOW())");
        List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertEquals(1, records.size());
        SourceRecord record = records.get(0);

        Struct source = (Struct) ((Struct) record.value()).get("source");
        assertEquals(source.get("version"), "0.1.8");
        assertEquals(source.get("connector"), "singlestore");
        assertEquals(source.get("name"), "singlestore_topic");
        assertNotNull(source.get("ts_ms"));
        assertEquals(source.get("snapshot"), "false");
        assertEquals(source.get("db"), "db");
        assertEquals(source.get("table"), "purchased");
        assertNotNull(source.get("txId"));
        assertNotNull(source.get("partitionId"));
        assertNotNull(source.get("offsets"));
        assertEquals(8, ((List<?>) source.get("offsets")).size());
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void noPrimaryKey() throws SQLException, InterruptedException {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("song"))) {
      Configuration config = defaultJdbcConfigWithTable("song");
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForStreamingToStart();
      try {
        conn.execute("INSERT INTO `song` VALUES ('Metallica', 'Enter Sandman')");
        conn.execute("INSERT INTO `song` VALUES ('AC/DC', 'Back In Black')");
        conn.execute("DELETE FROM `song` WHERE name = 'Enter Sandman'");

        List<SourceRecord> records = new ArrayList<>(consumeRecordsByTopic(4).allRecordsInOrder());
        records.sort(new Comparator<SourceRecord>() {
          @Override
          public int compare(SourceRecord r1, SourceRecord r2) {
            if (r1.value() == null) {
              return 1;
            }
            if (r2.value() == null) {
              return -1;
            }

            String op1 = ((Struct) r1.value()).getString("op");
            String op2 = ((Struct) r2.value()).getString("op");

            if (!Objects.equals(op1, op2)) {
              return op1.compareTo(op2);
            } else {
              return ((Struct) r1.value()).getStruct("after").getString("name")
                  .compareTo(((Struct) r2.value()).getStruct("after").getString("name"));
            }
          }
        });

        List<String> ids = new ArrayList<>();
        List<String> operations = Arrays.asList("c", "c", "d", null);

        assertEquals(4, records.size());
        for (int i = 0; i < records.size(); i++) {
          SourceRecord record = records.get(i);

          String operation = operations.get(i);
          Struct value = (Struct) record.value();
          if (operation == null) {
            assertNull(value);
          } else {
            assertEquals(operation, value.get("op"));
          }

          Struct key = (Struct) record.key();
          ids.add((String) key.get("internalId"));
        }

        assertEquals(ids.get(1), ids.get(2));
        assertEquals(ids.get(1), ids.get(3));
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void readSeveralOperations() throws SQLException, InterruptedException {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      Configuration config = defaultJdbcConfigWithTable("product");
      config = config.edit().withDefault("tombstones.on.delete", "false").build();
      conn.execute("SNAPSHOT DATABASE " + TEST_DATABASE + ";");
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForStreamingToStart();

      try {
        conn.execute("INSERT INTO `product` (`id`) VALUES (3)");
        conn.execute("DELETE FROM `product` WHERE `id` = 3");
        conn.execute("INSERT INTO `product` (`id`) VALUES (3)");
        conn.execute(
            "UPDATE `product` SET `createdByDate` = '2013-11-23 15:22:33' WHERE `id` = 3");

        List<SourceRecord> records = consumeRecordsByTopic(4).allRecordsInOrder();

        List<Long> ids = new ArrayList<>();
        List<String> operations = Arrays.asList("c", "d", "c", "u");

        assertEquals(4, records.size());
        for (int i = 0; i < records.size(); i++) {
          SourceRecord record = records.get(i);

          String operation = operations.get(i);
          Struct value = (Struct) record.value();
          if (operation == null) {
            assertNull(value);
          } else {
            assertEquals(operation, value.get("op"));
          }

          Struct key = (Struct) record.key();
          ids.add(key.getInt64("id"));
        }

        assertEquals(ids.get(0), ids.get(1));
        assertEquals(ids.get(2), ids.get(3));
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void filterColumns() throws SQLException, InterruptedException {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("person"))) {
      Configuration config = defaultJdbcConfigWithTable("person");
      config = config.edit().withDefault(SingleStoreConnectorConfig.COLUMN_INCLUDE_LIST,
          "db.person.name,db.person.age").build();
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForStreamingToStart();

      try {
        conn.execute("INSERT INTO `person` (`name`, `birthdate`, `age`, `salary`, `bitStr`) "
            + "VALUES ('Adalbert', '2001-04-11', 22, 100, 'a')");
        conn.execute("INSERT INTO `person` (`name`, `birthdate`, `age`, `salary`, `bitStr`) "
            + "VALUES ('Alice', '2001-04-11', 23, 100, 'a')");
        conn.execute("INSERT INTO `person` (`name`, `birthdate`, `age`, `salary`, `bitStr`) "
            + "VALUES ('Bob', '2001-04-11', 24, 100, 'a')");

        List<SourceRecord> records = new ArrayList<>(consumeRecordsByTopic(3).allRecordsInOrder());
        records.sort(new Comparator<SourceRecord>() {
          @Override
          public int compare(SourceRecord r1, SourceRecord r2) {
            return ((Struct) r1.key()).getString("name")
                .compareTo(((Struct) r2.key()).getString("name"));
          }
        });

        List<String> names = Arrays.asList("Adalbert", "Alice", "Bob");
        List<Integer> ages = Arrays.asList(22, 23, 24);
        List<String> operations = Arrays.asList("c", "c", "c");

        for (int i = 0; i < records.size(); i++) {
          SourceRecord record = records.get(i);

          String operation = operations.get(i);
          Struct value = (Struct) record.value();
          if (operation == null) {
            assertNull(value);
          } else {
            assertEquals(operation, value.get("op"));
          }

          value = value.getStruct("after");
          Set<String> columnNames = value.schema().fields().stream().map(field -> field.name())
              .collect(Collectors.toSet());
          assertEquals(new HashSet<>(Arrays.asList("name", "age")), columnNames);
          assertEquals(names.get(i), value.get("name"));
          assertEquals(ages.get(i), value.get("age"));
        }
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void internalId() throws SQLException, InterruptedException {
    try (SingleStoreConnection createTableConn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      createTableConn.execute("CREATE TABLE IF NOT EXISTS internalIdTable(a TEXT);"
          + "DELETE FROM internalIdTable WHERE 1 = 1;");
      try (SingleStoreConnection conn = new SingleStoreConnection(
          defaultJdbcConnectionConfigWithTable("internalIdTable"))) {
        Configuration config = defaultJdbcConfigWithTable("internalIdTable");
        config = config.edit()
            .withDefault(SingleStoreConnectorConfig.POPULATE_INTERNAL_ID, "true").build();
        conn.execute("SNAPSHOT DATABASE " + TEST_DATABASE + ";");
        start(SingleStoreConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        try {
          conn.execute("INSERT  INTO internalIdTable VALUES ('1')");

          List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
          assertEquals(1, records.size());
          for (SourceRecord record : records) {
            Struct value = (Struct) record.value();
            Struct key = (Struct) record.key();
            String internalId = value.getStruct("after").getString("internalId");
            assertEquals(key.getString("internalId"), internalId);
          }
        } finally {
          stopConnector();
        }
      }
    }
  }

  @Test
  public void testSkippedOperations() throws Exception {
    refreshTables();
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      Configuration config = defaultJdbcConfigWithTable("product");
      config = config.edit().withDefault(SingleStoreConnectorConfig.SKIPPED_OPERATIONS, "c")
          .withDefault(SingleStoreConnectorConfig.TOMBSTONES_ON_DELETE, "false").build();
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForStreamingToStart();

      try {
        conn.execute("INSERT INTO `product` (`id`) VALUES (1)");
        conn.execute("INSERT INTO `product` (`id`) VALUES (2)");
        conn.execute("INSERT INTO `product` (`id`) VALUES (3)");
        conn.execute("DELETE FROM `product` WHERE `id` = 1");
        conn.execute("UPDATE `product` SET `createdByDate` = '2013-11-23 15:22:33' WHERE `id` = 2");
        conn.execute("INSERT INTO `product` (`id`) VALUES (4)");

        List<SourceRecord> records = new ArrayList<>(consumeRecordsByTopic(2).allRecordsInOrder());
        records.sort(new Comparator<SourceRecord>() {
          @Override
          public int compare(SourceRecord r1, SourceRecord r2) {
            return ((Struct) r1.key()).getInt64("id")
                .compareTo(((Struct) r2.key()).getInt64("id"));
          }
        });

        List<String> operations = Arrays.asList("d", "u");
        assertEquals(2, records.size());
        for (int i = 0; i < records.size(); i++) {
          SourceRecord record = records.get(i);
          String operation = operations.get(i);
          Struct value = (Struct) record.value();
          assertEquals(operation, value.get("op"));
        }
      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void testStreamAfterInitialOnlySnapshot() throws SQLException, InterruptedException {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      Configuration config = defaultJdbcConfigWithTable("product");
      config = config.edit().withDefault(SingleStoreConnectorConfig.SNAPSHOT_MODE,
          SingleStoreConnectorConfig.SnapshotMode.INITIAL_ONLY).build();
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForSnapshotToBeCompleted();

      try {
        conn.execute("INSERT INTO `product` (`id`) VALUES (1)");
        conn.execute("INSERT INTO `product` (`id`) VALUES (2)");
        conn.execute("INSERT INTO `product` (`id`) VALUES (3)");
        conn.execute("DELETE FROM `product` WHERE `id` = 1");
        conn.execute("UPDATE `product` SET `createdByDate` = '2013-11-23 15:22:33' WHERE `id` = 2");
        conn.execute("INSERT INTO `product` (`id`) VALUES (4)");

        List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertEquals(0, records.size());

      } finally {
        stopConnector();
      }
    }
  }

  @Test
  public void testStaleOffset() throws Exception {
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfig())) {
      try {
        conn.execute(String.format("USE %s", TEST_DATABASE),
            "SET GLOBAL snapshots_to_keep=1",
            "SET GLOBAL snapshot_trigger_size=65536",
            "CREATE TABLE IF NOT EXISTS staleOffsets(a INT)",
            "DELETE FROM staleOffsets WHERE 1 > 0");

        Configuration config = defaultJdbcConfigWithTable("staleOffsets").edit()
            .withDefault("offset.flush.interval.ms", "20").build();

        start(SingleStoreConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        Thread.sleep(100);
        for (int i = 0; i < 10; i++) {
          conn.execute("INSERT INTO staleOffsets VALUES (123456789)");
        }
        Thread.sleep(100);

        List<SourceRecord> records = consumeRecordsByTopic(10).allRecordsInOrder();
        assertEquals(10, records.size());

        stopConnector();

        for (int i = 0; i < 80000; i++) {
          conn.execute("INSERT INTO staleOffsets VALUES (123456789)");
        }

        final TestAppender appender = new TestAppender();
        final Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
            "com.singlestore.debezium");
        appender.start();
        logger.addAppender(appender);
        try {
          start(SingleStoreConnector.class, config);
          assertConnectorIsRunning();
          consumeRecordsByTopic(1);
          assertConnectorNotRunning();
          assertTrue(appender.getLog().stream().anyMatch(event -> event.getMessage()
              .contains(
                  "Offset the connector is trying to resume from is considered stale.")));
        } finally {
          logger.detachAppender(appender);
        }
      } finally {
        conn.execute("SET GLOBAL snapshots_to_keep=2",
            "SET GLOBAL snapshot_trigger_size=2147483648");
        stopConnector();
      }
    }
  }

  @Test
  public void testPKInRowstore() throws Exception {
    try (SingleStoreConnection createTableConn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      createTableConn.execute(
          "CREATE ROWSTORE TABLE IF NOT EXISTS pkInRowstore(a INT, b TEXT, c TEXT, PRIMARY KEY(a, b));"
              + "DELETE FROM pkInRowstore WHERE 1 = 1;");
      try (SingleStoreConnection conn = new SingleStoreConnection(
          defaultJdbcConnectionConfigWithTable("pkInRowstore"))) {
        Configuration config = defaultJdbcConfigWithTable("pkInRowstore");
        config = config.edit().withDefault(SingleStoreConnectorConfig.COLUMN_INCLUDE_LIST,
            "db.pkInRowstore.a,db.pkInRowstore.c").build();
        conn.execute("SNAPSHOT DATABASE " + TEST_DATABASE + ";");
        start(SingleStoreConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        try {
          conn.execute("INSERT INTO pkInRowstore VALUES (2, 'd', 'e')");
          conn.execute("INSERT INTO pkInRowstore VALUES (1, 'b', 'c')");
          conn.execute("DELETE FROM pkInRowstore WHERE a = 1");

          List<SourceRecord> records = new ArrayList<>(
              consumeRecordsByTopic(3).allRecordsInOrder());
          assertEquals(3, records.size());
          records.sort(new Comparator<SourceRecord>() {
            @Override
            public int compare(SourceRecord r1, SourceRecord r2) {
              return ((Struct) r1.key()).getInt32("a")
                  .compareTo(((Struct) r2.key()).getInt32("a"));
            }
          });

          List<Integer> keyA = Arrays.asList(1, 1, 2);
          List<String> keyB = Arrays.asList("b", "b", "d");

          for (int i = 0; i < records.size(); i++) {
            SourceRecord record = records.get(i);
            Struct key = (Struct) record.key();
            assertEquals(keyA.get(i), key.getInt32("a"));
            assertEquals(keyB.get(i), key.getString("b"));
          }
        } finally {
          stopConnector();
        }
      }
    }
  }

  @Test
  public void testPKInColumnstore() throws Exception {
    try (SingleStoreConnection createTableConn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      createTableConn.execute(
          "CREATE TABLE IF NOT EXISTS pkInColumnstore(a INT, b TEXT, c TEXT, PRIMARY KEY(a, b));"
              + "DELETE FROM pkInColumnstore WHERE 1 = 1;"
              + "SNAPSHOT DATABASE " + TEST_DATABASE + ";"
      );
      try (SingleStoreConnection conn = new SingleStoreConnection(
          defaultJdbcConnectionConfigWithTable("pkInColumnstore"))) {
        Configuration config = defaultJdbcConfigWithTable("pkInColumnstore");
        config = config.edit()
            .withDefault(SingleStoreConnectorConfig.COLUMN_INCLUDE_LIST,
                "db.pkInColumnstore.a,db.pkInColumnstore.c")
            .withDefault("tombstones.on.delete", "false")
            .build();

        conn.execute("SNAPSHOT DATABASE " + TEST_DATABASE + ";");
        start(SingleStoreConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        try {
          conn.execute("INSERT INTO pkInColumnstore VALUES (2, 'd', 'e')");
          conn.execute("INSERT INTO pkInColumnstore VALUES (1, 'b', 'c')");
          conn.execute("DELETE FROM pkInColumnstore WHERE a = 1");

          List<SourceRecord> records = new ArrayList<>(
              consumeRecordsByTopic(3).allRecordsInOrder());
          assertEquals(3, records.size());
          records.sort(new Comparator<SourceRecord>() {
            @Override
            public int compare(SourceRecord r1, SourceRecord r2) {
              return ((Struct) r1.key()).getInt32("a")
                  .compareTo(((Struct) r2.key()).getInt32("a"));
            }
          });

          List<Integer> keyA = Arrays.asList(1, 1, 2);
          List<String> keyB = Arrays.asList("b", "b", "d");

          for (int i = 0; i < records.size(); i++) {
            SourceRecord record = records.get(i);
            Struct key = (Struct) record.key();
            assertEquals(keyA.get(i), key.getInt32("a"));
            assertEquals(keyB.get(i), key.getString("b"));
          }
        } finally {
          stopConnector();
        }
      }
    }
  }

  // Offset is validated successfully and offset is not reset, streaming should
  // proceed after connector is restarted
  @Test
  public void testValidOffsetInWhenNeededSnapshotMode() throws Exception {
    String table = "validOffsets1";
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfig())) {
      conn.execute(String.format("USE %s", TEST_DATABASE),
          String.format("DROP TABLE IF EXISTS %s", table),
          String.format("CREATE TABLE %s(a INT)", table));
      Configuration config = defaultJdbcConfigWithTable(table).edit()
          .withDefault(SingleStoreConnectorConfig.SNAPSHOT_MODE,
              SingleStoreConnectorConfig.SnapshotMode.WHEN_NEEDED)
          .build();
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      waitForStreamingToStart();

      Thread.sleep(100);
      for (int i = 0; i < 10; i++) {
        conn.execute(String.format("INSERT INTO %s VALUES (%s)", table, i));
      }

      Thread.sleep(100);
      List<SourceRecord> records = consumeRecordsByTopic(10).allRecordsInOrder();
      assertEquals(10, records.size());

      stopConnector();

      Thread.sleep(100);
      for (int i = 10; i < 20; i++) {
        conn.execute(String.format("INSERT INTO %s VALUES (%s)", table, i));
      }
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();

      Thread.sleep(100);
      records = consumeRecordsByTopic(10).allRecordsInOrder();
      assertEquals(10, records.size());
      // expected offset is not reset and stream type records are consumed
      assertNotNull("must be a stream type record",
          records.get(0).sourceOffset().get("snapshot_completed"));
      assertTrue("must be a stream type record",
          Boolean.parseBoolean(records.get(0).sourceOffset().get("snapshot_completed").toString()));
    } finally {
      stopConnector();
    }
  }

  // Offset is failed to validate and reset, after connector is restarted snapshot
  // reading should be executed
  @Test
  public void testStaleOffsetInWhenNeededSnapshotMode() throws Exception {
    String table = "staleOffsets2";
    try (SingleStoreConnection conn = new SingleStoreConnection(
        defaultJdbcConnectionConfig())) {
      try {
        conn.execute(String.format("USE %s", TEST_DATABASE),
            "SET GLOBAL snapshots_to_keep=1",
            "SET GLOBAL snapshot_trigger_size=65536",
            String.format("DROP TABLE IF EXISTS %s", table),
            String.format("CREATE TABLE %s(a INT)", table));
        Configuration config = defaultJdbcConfigWithTable(table).edit()
            .withDefault(OFFSET_FLUSH_INTERVAL_MS, 20)
            .withDefault(SingleStoreConnectorConfig.SNAPSHOT_MODE,
                SingleStoreConnectorConfig.SnapshotMode.WHEN_NEEDED)
            .build();
        start(SingleStoreConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        Thread.sleep(100);
        for (int i = 0; i < 10; i++) {
          conn.execute(String.format("INSERT INTO %s VALUES (%s)", table, i));
        }

        Thread.sleep(100);
        List<SourceRecord> records = consumeRecordsByTopic(10).allRecordsInOrder();
        assertEquals(10, records.size());

        stopConnector();

        Thread.sleep(100);
        for (int i = 10; i < 80010; i++) {
          conn.execute(String.format("INSERT INTO %s VALUES (%s)", table, i));
        }
        conn.execute(String.format("SNAPSHOT DATABASE %s", TEST_DATABASE));
        Thread.sleep(1000);
        start(SingleStoreConnector.class, config);
        assertConnectorIsRunning();

        records = consumeRecordsByTopic(80000).allRecordsInOrder();
        assertEquals(80000, records.size());
        // expected offset is reset and snapshot type records are consumed
        assertNotNull("must be a snapshot type record",
            records.get(0).sourceOffset().get("snapshot"));
        assertTrue("must be a snapshot type record",
            Boolean.parseBoolean(records.get(0).sourceOffset().get("snapshot").toString()));
      } finally {
        conn.execute("SET GLOBAL snapshots_to_keep=2",
            "SET GLOBAL snapshot_trigger_size=2147483648");
        stopConnector();
      }
    }
  }

  @Test
  public void noDataOffset() throws Exception {
    try (SingleStoreConnection createTableConn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      createTableConn.execute(
          "CREATE ROWSTORE TABLE IF NOT EXISTS noDataOffset(a INT, b TEXT);"
              + "DELETE FROM noDataOffset WHERE 1 = 1;"
              + "SNAPSHOT DATABASE db;"
              + "INSERT INTO noDataOffset VALUES(1, 'a');"
              + "SNAPSHOT DATABASE db");

      Configuration config = defaultJdbcConfigWithTable("noDataOffset");
      start(SingleStoreConnector.class, config);
      assertConnectorIsRunning();
      List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
      assertEquals(1, records.size());

      SourceRecord record = records.get(0);
      Struct value = (Struct) record.value();
      assertEquals(Integer.valueOf(1), value.getStruct("after").getInt32("a"));
      assertEquals("a", value.getStruct("after").getString("b"));
      List<String> offsets = value.getStruct("source").getArray("offsets");

      waitForStreamingToStart();
      stopConnector();

      config = config.edit()
          .withDefault(SingleStoreConnectorConfig.OFFSETS, String.join(",", offsets))
          .withDefault(SingleStoreConnectorConfig.SNAPSHOT_MODE, "no_data")
          .build();

      createTableConn.execute("INSERT INTO noDataOffset VALUES(2, 'b');");

      start(SingleStoreConnector.class, config);
      waitForStreamingToStart();
      assertConnectorIsRunning();

      records = consumeRecordsByTopic(1).allRecordsInOrder();
      assertEquals(1, records.size());

      record = records.get(0);
      value = (Struct) record.value();
      assertEquals(Integer.valueOf(2), value.getStruct("after").getInt32("a"));
      assertEquals("c", value.getString("op"));
      assertEquals("b", value.getStruct("after").getString("b"));
      stopConnector();
    }
  }

  @Test
  public void noData() throws Exception {
    try (SingleStoreConnection createTableConn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      createTableConn.execute(
          "CREATE ROWSTORE TABLE IF NOT EXISTS noData(a INT, b TEXT);"
              + "DELETE FROM noData WHERE 1 = 1;"
              + "SNAPSHOT DATABASE db;"
              + "INSERT INTO noData VALUES(1, 'a');"
              + "SNAPSHOT DATABASE db");

      Configuration config = defaultJdbcConfigWithTable("noData").edit()
          .withDefault(SingleStoreConnectorConfig.SNAPSHOT_MODE, "no_data")
          .build();

      start(SingleStoreConnector.class, config);
      waitForStreamingToStart();
      assertConnectorIsRunning();

      createTableConn.execute("INSERT INTO noData VALUES(2, 'b');");

      List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
      assertEquals(1, records.size());

      SourceRecord record = records.get(0);
      Struct value = (Struct) record.value();
      assertEquals(Integer.valueOf(2), value.getStruct("after").getInt32("a"));
      assertEquals("c", value.getString("op"));
      assertEquals("b", value.getStruct("after").getString("b"));
      stopConnector();
    }
  }

  @Test
  public void noDataResume() throws Exception {
    try (SingleStoreConnection createTableConn = new SingleStoreConnection(
        defaultJdbcConnectionConfigWithTable("product"))) {
      createTableConn.execute(
          "CREATE ROWSTORE TABLE IF NOT EXISTS noDataResume(a INT, b TEXT);"
              + "DELETE FROM noDataResume WHERE 1 = 1;"
              + "SNAPSHOT DATABASE db;"
              + "INSERT INTO noDataResume VALUES(1, 'a');"
              + "SNAPSHOT DATABASE db");

      Configuration config = defaultJdbcConfigWithTable("noDataResume").edit()
          .withDefault(SingleStoreConnectorConfig.SNAPSHOT_MODE, "no_data")
          .build();

      start(SingleStoreConnector.class, config);
      waitForStreamingToStart();
      assertConnectorIsRunning();

      createTableConn.execute("INSERT INTO noDataResume VALUES(2, 'b');");

      List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
      assertEquals(1, records.size());

      SourceRecord record = records.get(0);
      Struct value = (Struct) record.value();
      assertEquals(Integer.valueOf(2), value.getStruct("after").getInt32("a"));
      assertEquals("c", value.getString("op"));
      assertEquals("b", value.getStruct("after").getString("b"));
      stopConnector();

      createTableConn.execute("INSERT INTO noDataResume VALUES(3, 'c');");

      start(SingleStoreConnector.class, config);
      waitForStreamingToStart();
      assertConnectorIsRunning();

      createTableConn.execute("INSERT INTO noDataResume VALUES(4, 'd');");

      records = consumeRecordsByTopic(2).allRecordsInOrder();
      assertEquals(2, records.size());

      record = records.get(0);
      value = (Struct) record.value();
      assertEquals(Integer.valueOf(3), value.getStruct("after").getInt32("a"));
      assertEquals("c", value.getString("op"));
      assertEquals("c", value.getStruct("after").getString("b"));
      stopConnector();

      record = records.get(1);
      value = (Struct) record.value();
      assertEquals(Integer.valueOf(4), value.getStruct("after").getInt32("a"));
      assertEquals("c", value.getString("op"));
      assertEquals("d", value.getStruct("after").getString("b"));
      stopConnector();
    }
  }
}
