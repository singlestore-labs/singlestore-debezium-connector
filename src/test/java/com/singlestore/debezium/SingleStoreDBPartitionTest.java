package com.singlestore.debezium;

import io.debezium.connector.common.AbstractPartitionTest;


public class SingleStoreDBPartitionTest extends AbstractPartitionTest<SingleStoreDBPartition>  {
    @Override
    protected SingleStoreDBPartition createPartition1() {
        return new SingleStoreDBPartition("server1", "database1");
    }

    @Override
    protected SingleStoreDBPartition createPartition2() {
        return new SingleStoreDBPartition("server2", "database2");
    }
}
