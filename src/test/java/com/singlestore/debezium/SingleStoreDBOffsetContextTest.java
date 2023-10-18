package com.singlestore.debezium;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

import com.singlestore.debezium.SingleStoreDBOffsetContext.Loader;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

public class SingleStoreDBOffsetContextTest {
    @Test
    public void saveAndLoad() {
        SingleStoreDBConnectorConfig conf = new SingleStoreDBConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                        .with(SingleStoreDBConnectorConfig.DATABASE_NAME, "database")
                        .build());

        SingleStoreDBOffsetContext offsetContext = new SingleStoreDBOffsetContext(conf, null, null, null, false, false);

        offsetContext.event(TableId.parse("db.t", true), Instant.parse("2018-11-30T18:35:24.00Z"));
        offsetContext.update(1, "3", Arrays.asList("1", "10", null, "2"));
        offsetContext.preSnapshotCompletion();

        Map<String, ?> offset = offsetContext.getOffset();

        Loader loader = new SingleStoreDBOffsetContext.Loader(conf);
        SingleStoreDBOffsetContext loadedOffsetContext = loader.load(offset);

        assertEquals(loadedOffsetContext.partitionId(), (Integer)1);
        assertEquals(loadedOffsetContext.txId(), "3");
        assertEquals(loadedOffsetContext.offsets(), Arrays.asList("1", "10", null, "2"));
        assertFalse(loadedOffsetContext.isSnapshotRunning());
    }
}
