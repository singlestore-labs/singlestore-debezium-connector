package com.singlestore.debezium;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

import com.singlestore.debezium.SingleStoreOffsetContext.Loader;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

public class SingleStoreOffsetContextTest {
    @Test
    public void saveAndLoad() {
        SingleStoreConnectorConfig conf = new SingleStoreConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                        .with(SingleStoreConnectorConfig.DATABASE_NAME, "database")
                        .build());

        SingleStoreOffsetContext offsetContext = new SingleStoreOffsetContext(conf, null, null, Arrays.asList(null, null, null, null), false, false);

        offsetContext.event(TableId.parse("db.t", true), Instant.parse("2018-11-30T18:35:24.00Z"));
        offsetContext.update(0, "0", "1");
        offsetContext.update(3, "1", "2");
        offsetContext.update(1, "3", "10");
        offsetContext.preSnapshotCompletion();

        Map<String, Object> offset = (Map<String, Object>)offsetContext.getOffset();

        Loader loader = new SingleStoreOffsetContext.Loader(conf);
        SingleStoreOffsetContext loadedOffsetContext = loader.load(offset);

        assertEquals(loadedOffsetContext.partitionId(), (Integer)1);
        assertEquals(loadedOffsetContext.txId(), "3");
        assertEquals(loadedOffsetContext.offsets(), Arrays.asList("1", "10", null, "2"));
        assertFalse(loadedOffsetContext.isSnapshotRunning());

        offset.put("partitionId", Long.valueOf(2));
        loadedOffsetContext = loader.load(offset);
        assertEquals(loadedOffsetContext.partitionId(), (Integer)2);

        offset.put("partitionId", null);
        loadedOffsetContext = loader.load(offset);
        assertNull(loadedOffsetContext.partitionId());
    }
}
