package com.singlestore.debezium;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.Arrays;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

public class SingleStoreDBEventMetadataProviderTest {
    
    TableId table;
    SingleStoreDBOffsetContext offsetContext;
    Struct value;
    SingleStoreDBEventMetadataProvider provider = new SingleStoreDBEventMetadataProvider();

    @Before
    public void init() {
        SingleStoreDBConnectorConfig conf = new SingleStoreDBConnectorConfig(
        Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(SingleStoreDBConnectorConfig.DATABASE_NAME, "database")
                .build());

        SingleStoreDBOffsetContext offsetContext = new SingleStoreDBOffsetContext(conf, null, null, null, false, false);

        table = TableId.parse("db.t", true);
        offsetContext.event(table, Instant.parse("2018-11-30T18:35:24.00Z"));
        offsetContext.update(1, "3", Arrays.asList("1", "10", null, "2"));
        offsetContext.preSnapshotCompletion();

        Schema schema = SchemaBuilder.struct().field(Envelope.FieldName.SOURCE, offsetContext.getSourceInfoSchema()).build();
        value = new Struct(schema);
        value.put(Envelope.FieldName.SOURCE, offsetContext.getSourceInfo());
    }

    @Test
    public void timestamp() {
        assertEquals(provider.getEventTimestamp(table, offsetContext, null, value), Instant.parse("2018-11-30T18:35:24.00Z"));
    }

    @Test
    public void transactionId() {
        assertEquals(provider.getTransactionId(table, offsetContext, null, value), "3");
    }

    @Test
    public void sourcePosition() {
        assertEquals(provider.getEventSourcePosition(table, offsetContext, null, value), 
            Collect.hashMapOf(SourceInfo.OFFSETS_KEY, "1,10,null,2"));
    }
}
