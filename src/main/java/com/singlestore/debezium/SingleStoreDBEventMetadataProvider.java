package com.singlestore.debezium;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

public class SingleStoreDBEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEventTimestamp'");
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key,
            Struct value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEventSourcePosition'");
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTransactionId'");
    }
    
}
