package com.singlestore.debezium;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaAndValueField;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class SnapshotIT extends IntegrationTestBase {

    @Before
    public void initTestData() {
        String statements = "DROP DATABASE IF EXISTS " + TEST_DATABASE + ";" +
                "CREATE DATABASE " + TEST_DATABASE + ";" +
                "DROP TABLE IF EXISTS " + TEST_DATABASE + ".A;" +
                "DROP TABLE IF EXISTS " + TEST_DATABASE + ".B;" +
                "CREATE TABLE " + TEST_DATABASE + ".A (pk INT, aa VARCHAR(10), PRIMARY KEY(pk));" +
                "CREATE TABLE " + TEST_DATABASE + ".B (aa INT, bb VARCHAR(20));" +
                "INSERT INTO " + TEST_DATABASE + ".B VALUES(0, 'test0');" +
                "INSERT INTO " + TEST_DATABASE + ".A VALUES(0, 'test0');" +
                "INSERT INTO " + TEST_DATABASE + ".A VALUES(4, 'test4');" +
                "INSERT INTO " + TEST_DATABASE + ".A VALUES(1, 'test1');" +
                "INSERT INTO " + TEST_DATABASE + ".A VALUES(2, 'test2');" +
                "UPDATE " + TEST_DATABASE + ".B SET bb = 'testUpdated' WHERE aa = 0;" +
                "DELETE FROM " + TEST_DATABASE + ".A WHERE pk = 4;" +
                "SNAPSHOT DATABASE " + TEST_DATABASE + ";";
        execute(statements);
    }

    @Test
    public void testSnapshotA() throws Exception {
        final Configuration config = defaultJdbcConfigWithTable("A");

        start(SingleStoreDBConnector.class, config);
        assertConnectorIsRunning();

        try {
            final SourceRecords recordsA = consumeRecordsByTopic(3);
            final List<SourceRecord> table1 = recordsA.recordsForTopic(TEST_TOPIC_PREFIX + "." + TEST_DATABASE + ".A")
                    .stream().sorted(Comparator.comparingInt(v -> (Integer) ((Struct)((Struct) v.value()).get("after")).get("pk"))).collect(Collectors.toList());
            assertThat(table1).hasSize(3);
            
            for (int i = 0; i < 3; i++) {
                final SourceRecord record1 = table1.get(i);
                final List<SchemaAndValueField> expectedRow1 = Arrays.asList(
                        new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).required().build(), i),
                        new SchemaAndValueField("aa", Schema.OPTIONAL_STRING_SCHEMA, "test" + i));
                final Struct key1 = (Struct) record1.key();
                final Struct value1 = (Struct) record1.value();
                assertNotNull(key1.get("internalId"));
                assertEquals(Schema.Type.STRUCT, key1.schema().type());
                assertEquals(Schema.Type.INT64, key1.schema().fields().get(0).schema().type());
                assertRecord((Struct) value1.get("after"), expectedRow1);
                assertThat(record1.sourceOffset())
                        .extracting("snapshot").containsExactly(true);
//              assertThat(record1.sourceOffset())
//                        .extracting("snapshot_completed").containsExactly(i == 2);
                assertNull(value1.get("before"));
            }
        } finally {
            stopConnector();
        }
    }

    @Test
    public void testSnapshotB() throws Exception {
        final Configuration config = defaultJdbcConfigWithTable("B");

        start(SingleStoreDBConnector.class, config);
        assertConnectorIsRunning();
        
        try {
            final SourceRecords recordsB = consumeRecordsByTopic(1);
            final List<SourceRecord> table2 = recordsB.recordsForTopic(TEST_TOPIC_PREFIX + "." + TEST_DATABASE + ".B");
            assertThat(table2).hasSize(1);
            final SourceRecord record1 = table2.get(0);
            final List<SchemaAndValueField> expectedRow1 = Arrays.asList(
                    new SchemaAndValueField("aa", Schema.OPTIONAL_INT32_SCHEMA, 0),
                    new SchemaAndValueField("bb", Schema.OPTIONAL_STRING_SCHEMA, "testUpdated"));
            final Struct key1 = (Struct) record1.key();
            final Struct value1 = (Struct) record1.value();
            assertRecord((Struct) value1.get("after"), expectedRow1);
            assertThat(record1.sourceOffset())
                    .extracting("snapshot").containsExactly(true);
//          assertThat(record1.sourceOffset())
//                  .extracting("snapshot_completed").containsExactly(false);
            assertNull(value1.get("before"));
            assertNotNull(key1.get("internalId"));
            assertEquals(Schema.Type.STRUCT, key1.schema().type());
            assertEquals(Schema.Type.INT64, key1.schema().fields().get(0).schema().type());

        } finally {
            stopConnector();
        }
    }

    @Test
    public void testSnapshotFilter() throws InterruptedException {
        final Configuration config = defaultJdbcConfigWithTable("B");

        start(SingleStoreDBConnector.class, config);
        assertConnectorIsRunning();
        final SourceRecords recordsB = consumeRecordsByTopic(1);
        final List<SourceRecord> table2 = recordsB.recordsForTopic(TEST_TOPIC_PREFIX + "." + TEST_DATABASE + ".B");
        assertThat(table2).hasSize(1);
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> {
            schemaAndValueField.assertFor(record);
            
        });
    }

    @Override
    protected int consumeRecords(int numberOfRecords, Consumer<SourceRecord> recordConsumer) throws InterruptedException {
        int breakAfterNulls = waitTimeForRecordsAfterNulls();
        return this.consumeRecords(numberOfRecords, breakAfterNulls, recordConsumer, false);
    }

    @Test
    public void filterColumns() throws SQLException, InterruptedException {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfigWithTable("A"))) {

            Configuration config = defaultJdbcConfigWithTable("A");
            config = config.edit()
            .withDefault(SingleStoreDBConnectorConfig.TABLE_NAME, "A")
            .withDefault(SingleStoreDBConnectorConfig.COLUMN_INCLUDE_LIST, "aa")
            .build();

            start(SingleStoreDBConnector.class, config);
            assertConnectorIsRunning();
            try {

                List<SourceRecord> records = consumeRecordsByTopic(3).allRecordsInOrder();
                
                List<String> values = Arrays.asList(new String[]{"test0", "test1", "test2"});
                List<String> operations = Arrays.asList(new String[]{"r", "r", "r"});

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
                    Set<String> columnNames = value.schema()
                        .fields()
                        .stream()
                        .map(field -> field.name())
                        .collect(Collectors.toSet());
                    assertEquals(new HashSet<>(Arrays.asList("aa")), columnNames);
                    assertEquals(values.get(i), value.get("aa"));
                }
            } finally {
                stopConnector();
            }    
        }
    }

}
