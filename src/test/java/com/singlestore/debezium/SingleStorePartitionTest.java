package com.singlestore.debezium;

import io.debezium.connector.common.AbstractPartitionTest;


public class SingleStorePartitionTest extends AbstractPartitionTest<SingleStorePartition>  {
    @Override
    protected SingleStorePartition createPartition1() {
        return new SingleStorePartition("server1", "database1");
    }

    @Override
    protected SingleStorePartition createPartition2() {
        return new SingleStorePartition("server2", "database2");
    }
}
