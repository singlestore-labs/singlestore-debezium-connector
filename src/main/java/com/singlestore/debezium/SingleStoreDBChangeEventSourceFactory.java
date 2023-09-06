package com.singlestore.debezium;

import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;

public class SingleStoreDBChangeEventSourceFactory  implements ChangeEventSourceFactory<SingleStoreDBPartition, SingleStoreDBOffsetContext> {
    
}
