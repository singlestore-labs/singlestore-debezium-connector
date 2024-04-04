package com.singlestore.debezium;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class SingleStorePartition extends AbstractPartition {

  private static final String SERVER_PARTITION_KEY = "server";

  private final String serverName;

  public SingleStorePartition(String serverName, String databaseName) {
    super(databaseName);
    this.serverName = serverName;
  }

  @Override
  public Map<String, String> getSourcePartition() {
    return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final SingleStorePartition other = (SingleStorePartition) obj;
    return Objects.equals(serverName, other.serverName);
  }

  @Override
  public int hashCode() {
    return serverName.hashCode();
  }

  @Override
  public String toString() {
    return "SingleStorePartition [sourcePartition=" + getSourcePartition() + "]";
  }

  public static class Provider implements Partition.Provider<SingleStorePartition> {

    private final SingleStoreConnectorConfig connectorConfig;
    private final Configuration taskConfig;

    public Provider(SingleStoreConnectorConfig connectorConfig, Configuration taskConfig) {
      this.connectorConfig = connectorConfig;
      this.taskConfig = taskConfig;
    }

    @Override
    public Set<SingleStorePartition> getPartitions() {
      return Collections.singleton(new SingleStorePartition(
          connectorConfig.getLogicalName(), connectorConfig.databaseName()));
    }
  }
}
