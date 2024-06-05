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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
            "-128, " +  // tinyintColumn
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
            "'1000-01-01 00:00:00', " +  // datetimeColumn
            "'1000-01-01 00:00:00.000000', " + // datetime6Column
            "'1970-01-01 00:00:01', " +  // timestampColumn
            "'1970-01-01 00:00:01.000000', " +  // timestamp6Column
            "1901, " +  // yearColumn
            "12345678901234567890123456789012345.123456789012345678901234567891, " +
            // decimalColumn
            "1234567890, " + // decColumn
            "1234567890, " + // fixedColumn
            "1234567890, " +  // numericColumn
            "'a', " + // charColumn
            "'abc', " +  // mediumtextColumn
            "'a', " + // binaryColumn
            "'abc', " +  // varcharColumn
            "'abc', " +  // varbinaryColumn
            "'abc', " +  // longtextColumn
            "'abc', " +  // textColumn
            "'abc', " +  // tinytextColumn
            "'abc', " +  // longblobColumn
            "'abc', " +  // mediumblobColumn
            "'abc', " +  // blobColumn
            "'abc', " +  // tinyblobColumn
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
        assertEquals((int) -8388608, after.get("mediumintColumn"));
        assertEquals((short) -32768, after.get("smallintColumn"));
        assertEquals((int) -2147483648, after.get("intColumn"));
        assertEquals((int) -2147483648, after.get("integerColumn"));
        assertEquals((long) -9223372036854775808l, after.get("bigintColumn"));
        assertEquals((float) -100.01, after.get("floatColumn"));
        assertEquals((double) -1000.01, after.get("doubleColumn"));
        assertEquals((double) -1000.01, after.get("realColumn"));
        assertEquals((int) -354285, after.get("dateColumn"));
        assertEquals((int) 0, after.get("timeColumn"));
        assertEquals((long) 0, after.get("time6Column"));
        assertEquals((long) -30610224000000l, after.get("datetimeColumn"));
        assertEquals((long) -30610224000000000l, after.get("datetime6Column"));
        assertEquals((long) 1000, after.get("timestampColumn"));
        assertEquals((long) 1000000, after.get("timestamp6Column"));
        assertEquals((int) 1901, after.get("yearColumn"));
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
        assertEquals(source.get("version"), "0.1.3");
        assertEquals(source.get("connector"), "singlestore");
        assertEquals(source.get("name"), "singlestore_topic");
        assertNotNull(source.get("ts_ms"));
        assertEquals(source.get("snapshot"), "false");
        assertEquals(source.get("db"), "db");
        assertEquals(source.get("table"), "purchased");
        assertNotNull(source.get("txId"));
        assertEquals(source.get("partitionId"), 0);
        assertNotNull(source.get("offsets"));
        assertEquals(3, ((List) source.get("offsets")).size());
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

        List<SourceRecord> records = consumeRecordsByTopic(4).allRecordsInOrder();

        List<Long> ids = new ArrayList<>();
        List<String> operations = Arrays.asList(new String[]{"c", "c", "d", null});

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
          ids.add((Long) key.get("internalId"));
        }

        assertEquals(ids.get(0), ids.get(2));
        assertEquals(ids.get(0), ids.get(3));
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
        conn.execute("INSERT INTO `product` (`id`) VALUES (1)");
        conn.execute("INSERT INTO `product` (`id`) VALUES (2)");
        conn.execute("INSERT INTO `product` (`id`) VALUES (3)");
        conn.execute("DELETE FROM `product` WHERE `id` = 1");
        conn.execute("UPDATE `product` SET `createdByDate` = '2013-11-23 15:22:33' WHERE `id` = 2");
        conn.execute("INSERT INTO `product` (`id`) VALUES (4)");

        List<SourceRecord> records = consumeRecordsByTopic(6).allRecordsInOrder();

        List<Long> ids = new ArrayList<>();
        List<String> operations = Arrays.asList(new String[]{"c", "c", "c", "d", "u", "c"});

        assertEquals(6, records.size());
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
          ids.add(key.getInt64("internalId"));
        }

        assertEquals(ids.get(0), ids.get(3));
        assertEquals(ids.get(1), ids.get(4));
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

        List<SourceRecord> records = consumeRecordsByTopic(3).allRecordsInOrder();

        List<String> names = Arrays.asList(new String[]{"Adalbert", "Alice", "Bob"});
        List<Integer> ages = Arrays.asList(new Integer[]{22, 23, 24});
        List<String> operations = Arrays.asList(new String[]{"c", "c", "c"});

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
      createTableConn.execute("CREATE TABLE IF NOT EXISTS internalIdTable(a INT);"
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
          conn.execute("INSERT  INTO internalIdTable VALUES (1)");

          List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
          assertEquals(1, records.size());
          for (SourceRecord record : records) {
            Struct value = (Struct) record.value();
            Struct key = (Struct) record.key();
            Long internalId = value.getStruct("after").getInt64("internalId");
            assertEquals(key.getInt64("internalId"), internalId);
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

        List<SourceRecord> records = consumeRecordsByTopic(2).allRecordsInOrder();

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
            "CREATE TABLE staleOffsets(a INT)"
        );

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

        for (int i = 0; i < 10000; i++) {
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
          assertThrows("Expected streaming to fail",
              org.awaitility.core.ConditionTimeoutException.class, this::waitForStreamingToStart);

          assertTrue(appender.getLog().stream().anyMatch(event -> event.getMessage()
              .contains(
                  "Offset the connector is trying to resume from is considered stale.")));
        } finally {
          logger.detachAppender(appender);
        }
      } finally {
        conn.execute("SET GLOBAL snapshots_to_keep=2",
            "SET GLOBAL snapshot_trigger_size=2147483648"
        );
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

          List<SourceRecord> records = consumeRecordsByTopic(3).allRecordsInOrder();
          assertEquals(3, records.size());

          List<Integer> keyA = Arrays.asList(2, 1, 1);
          List<String> keyB = Arrays.asList("d", "b", "b");

          for (int i = 0; i < records.size(); i++) {
            SourceRecord record = records.get(i);
            Struct key = (Struct) record.key();
            assertEquals(key.getInt32("a"), keyA.get(i));
            assertEquals(key.getString("b"), keyB.get(i));
          }
        } finally {
          stopConnector();
        }
      }
    }
  }

  //Offset is validated successfully and offset is not reset, streaming should proceed after connector is restarted
  @Test
  public void testValidOffsetInWhenNeededSnapshotMode() throws Exception{
    String table = "validOffsets1";
    try (SingleStoreConnection conn = new SingleStoreConnection(
            defaultJdbcConnectionConfig())) {
      conn.execute(String.format("USE %s", TEST_DATABASE),
              String.format("DROP TABLE IF EXISTS %s", table),
              String.format("CREATE TABLE %s(a INT)", table)
      );
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
      //expected offset is not reset and stream type records are consumed
      assertNotNull("must be a stream type record", records.get(0).sourceOffset().get("snapshot_completed"));
      assertTrue("must be a stream type record", Boolean.parseBoolean(records.get(0).sourceOffset().get("snapshot_completed").toString()));
    } finally {
      stopConnector();
    }
  }

  //Offset is failed to validate and reset, after connector is restarted snapshot reading should be executed
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
                String.format("CREATE TABLE %s(a INT)", table)
        );
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
        for (int i = 10; i < 10010; i++) {
          conn.execute(String.format("INSERT INTO %s VALUES (%s)", table, i));
        }
        Thread.sleep(100);
        start(SingleStoreConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);
        records = consumeRecordsByTopic(10000).allRecordsInOrder();
        //expected offset is reset and snapshot type records are consumed
        assertNotNull("must be a snapshot type record", records.get(0).sourceOffset().get("snapshot"));
        assertTrue("must be a snapshot type record", Boolean.parseBoolean(records.get(0).sourceOffset().get("snapshot").toString()));
      } finally {
        conn.execute("SET GLOBAL snapshots_to_keep=2",
                "SET GLOBAL snapshot_trigger_size=2147483648"
        );
        stopConnector();
      }
    }
  }
}
