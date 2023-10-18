package com.singlestore.debezium;


import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.VerifyRecord;
import io.debezium.relational.TableId;

public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        source = new SourceInfo(new SingleStoreDBConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                        .with(SingleStoreDBConnectorConfig.DATABASE_NAME, "database")
                        .build()));
        source.update(10, "123", Arrays.asList("1", "2", null, "3"));
        source.update(TableId.parse("db.t", true), Instant.parse("2018-11-30T18:35:24.00Z"));
        source.setSnapshot(SnapshotRecord.TRUE);
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }

    @Test
    public void nameIsPresent() {
        assertThat(source.struct().getString("name")).isEqualTo("server");
    }

    @Test
    public void timestampIsPresent() {
        assertThat(source.struct().getInt64("ts_ms")).isEqualTo(Instant.parse("2018-11-30T18:35:24.00Z").toEpochMilli());
    }

    @Test
    public void snapshotIsPresent() {
        assertThat(source.struct().getString("snapshot")).isEqualTo("true");
    }

    @Test
    public void dbIsPresent() {
        assertThat(source.struct().getString("db")).isEqualTo("db");
    }

    @Test
    public void sequenceIsNull() {
        assertThat(source.struct().getString("sequence")).isNull();
    }

    @Test
    public void tableIsPresent() {
        assertThat(source.struct().getString("table")).isEqualTo("t");
    }

    @Test
    public void txIdIsPresent() {
        assertThat(source.struct().getString("txId")).isEqualTo("123");
    }

    @Test
    public void partitionIdIsPresent() {
        assertThat(source.struct().getInt32("partitionId")).isEqualTo(10);
    }

    @Test
    public void offsetsIsPresent() {
        assertThat(source.struct().getArray("offsets")).isEqualTo(Arrays.asList("1", "2", null, "3"));
    }

    @Test
    public void schemaIsCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("com.singlestore.debezium.Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("txId", Schema.STRING_SCHEMA)
                .field("partitionId", Schema.INT32_SCHEMA)
                .field("offsets", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
                .build();

        Schema s = source.struct().schema();
        VerifyRecord.assertConnectSchemasAreEqual(null, s, schema);
    }    
}
