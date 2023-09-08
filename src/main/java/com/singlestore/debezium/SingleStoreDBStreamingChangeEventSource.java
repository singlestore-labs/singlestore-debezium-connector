package com.singlestore.debezium;

import io.debezium.pipeline.source.spi.StreamingChangeEventSource;

public class SingleStoreDBStreamingChangeEventSource implements StreamingChangeEventSource<SingleStoreDBPartition, SingleStoreDBOffsetContext>{

    @Override
    public void execute(ChangeEventSourceContext context, SingleStoreDBPartition partition,
            SingleStoreDBOffsetContext offsetContext) throws InterruptedException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execute'");
    }
    
}
