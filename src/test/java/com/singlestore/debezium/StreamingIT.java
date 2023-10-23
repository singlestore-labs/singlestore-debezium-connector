package com.singlestore.debezium;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.config.Configuration;

public class StreamingIT extends IntegrationTestBase {
    @Test
    public void canReadAllTypes() throws SQLException, InterruptedException {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfigWithDatabase())) {
            conn.execute(String.format("SNAPSHOT DATABASE %s", TEST_DATABASE));

            conn.execute("INSERT INTO `allTypesTable` VALUES (\n" + 
            "TRUE, " + // boolColumn
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
            "12345678901234567890123456789012345.123456789012345678901234567891, " + // decimalColumn
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
//            "'POLYGON((1 1,2 1,2 2, 1 2, 1 1))', " + // geographyColumn TODO: PLAT-6907 test GEOGRAPHY datatype
            "'POINT(1.50000003 1.50000000)')" // geographypointColumn
            );

            Configuration config = defaultJdbcConfigWithDatabase();
            config = config.edit()
            .withDefault(SingleStoreDBConnectorConfig.SNAPSHOT_MODE, "schema_only")
            .withDefault(SingleStoreDBConnectorConfig.TABLE_INCLUDE_LIST, "db.allTypesTable")
            .build();

            start(SingleStoreDBConnector.class, config);
            assertConnectorIsRunning();

            try {
                List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
                assertEquals(1, records.size());

                SourceRecord record = records.get(0);
                Struct value = (Struct) record.value();
                Struct after = (Struct) value.get("after");

                // TODO: PLAT-6909 handle BOOL columns as boolean
                assertEquals((short)1, after.get("boolColumn"));
                assertEquals((short)1, after.get("booleanColumn"));
                // TODO: PLAT-6910 BIT type is returned in reversed order
                assertArrayEquals("hgfedcba".getBytes(), (byte[])after.get("bitColumn"));
                assertEquals((short)-128, after.get("tinyintColumn"));
                assertEquals((int)-8388608, after.get("mediumintColumn"));
                assertEquals((short)-32768, after.get("smallintColumn"));
                assertEquals((int)-2147483648, after.get("intColumn"));
                assertEquals((int)-2147483648, after.get("integerColumn"));
                assertEquals((long)-9223372036854775808l, after.get("bigintColumn"));
                assertEquals((float)-100.01, after.get("floatColumn"));
                assertEquals((double)-1000.01, after.get("doubleColumn"));
                assertEquals((double)-1000.01, after.get("realColumn"));
                assertEquals((int)-354285, after.get("dateColumn"));
                assertEquals((int)0, after.get("timeColumn"));
                assertEquals((long)0, after.get("time6Column"));
                assertEquals((long)-30610224000000l, after.get("datetimeColumn"));
                assertEquals((long)-30610224000000000l, after.get("datetime6Column"));
                assertEquals((long)1000, after.get("timestampColumn"));
                assertEquals((long)1000000, after.get("timestamp6Column"));
                assertEquals((int)1901, after.get("yearColumn"));
                assertEquals(new BigDecimal("12345678901234567890123456789012345.123456789012345678901234567891"), after.get("decimalColumn"));
                assertEquals(new BigDecimal("1234567890"), after.get("decColumn"));
                assertEquals(new BigDecimal("1234567890"), after.get("fixedColumn"));
                assertEquals(new BigDecimal("1234567890"), after.get("numericColumn"));
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
                String geographyPointValue = "POINT(1.50000003 1.50000000)";
                SingleStoreDBGeometry singleStoreDBgeographyPointValue = SingleStoreDBGeometry.fromEkt(geographyPointValue);
                assertArrayEquals((byte[]) ((Struct)after.get("geographypointColumn")).get("wkb"), 
                    singleStoreDBgeographyPointValue.getWkb());
            } finally {
                stopConnector();
            }
        }
    }

    @Test
    public void noPrimaryKey() throws SQLException, InterruptedException {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfigWithDatabase())) {
            conn.execute(String.format("SNAPSHOT DATABASE %s", TEST_DATABASE));
            conn.execute("INSERT INTO `song` VALUES ('Metallica', 'Enter Sandman')");
            conn.execute("INSERT INTO `song` VALUES ('AC/DC', 'Back In Black')");
            conn.execute("DELETE FROM `song` WHERE name = 'Enter Sandman'");

            Configuration config = defaultJdbcConfigWithDatabase();
            config = config.edit()
            .withDefault(SingleStoreDBConnectorConfig.SNAPSHOT_MODE, "schema_only")
            .withDefault(SingleStoreDBConnectorConfig.TABLE_INCLUDE_LIST, "db.song")
            .build();

            start(SingleStoreDBConnector.class, config);
            assertConnectorIsRunning();

            try {
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
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfigWithDatabase())) {
            conn.execute(String.format("SNAPSHOT DATABASE %s", TEST_DATABASE));
            conn.execute("INSERT INTO `product` (`id`) VALUES (1)");
            conn.execute("INSERT INTO `product` (`id`) VALUES (2)");
            conn.execute("INSERT INTO `product` (`id`) VALUES (3)");
            conn.execute("DELETE FROM `product` WHERE `id` = 1");
            conn.execute("UPDATE `product` SET `createdByDate` = '2013-11-23 15:22:33' WHERE `id` = 2");
            conn.execute("INSERT INTO `product` (`id`) VALUES (4)");

            Configuration config = defaultJdbcConfigWithDatabase();
            config = config.edit()
            .withDefault(SingleStoreDBConnectorConfig.SNAPSHOT_MODE, "schema_only")
            .withDefault(SingleStoreDBConnectorConfig.TABLE_INCLUDE_LIST, "db.product")
            .build();

            start(SingleStoreDBConnector.class, config);
            assertConnectorIsRunning();

            try {
                List<SourceRecord> records = consumeRecordsByTopic(7).allRecordsInOrder();
                
                List<Integer> ids = Arrays.asList(new Integer[]{1, 2, 3, 
                    0, // TODO: PLAT-6906 get PK for DELETE events
                    0, // TODO: PLAT-6906 get PK for DELETE events
                    2, 4});
                List<String> operations = Arrays.asList(new String[]{"c", "c", "c", "d", null, "u", "c"});

                assertEquals(7, records.size());
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
                    assertEquals(ids.get(i), key.get("id"));
                }
            } finally {
                stopConnector();
            }    
        }
    }
}