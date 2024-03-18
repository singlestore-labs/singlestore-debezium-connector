package com.singlestore.debezium.metrics;

import com.singlestore.debezium.SingleStorePartition;
import com.singlestore.debezium.events.ObserveStreamingStartedEvent;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class SingleStoreStreamingChangeEventSourceMetrics extends
    DefaultStreamingChangeEventSourceMetrics<SingleStorePartition> {

  public SingleStoreStreamingChangeEventSourceMetrics(
      CdcSourceTaskContext taskContext,
      ChangeEventQueueMetrics changeEventQueueMetrics,
      EventMetadataProvider metadataProvider) {
    super(taskContext, changeEventQueueMetrics, metadataProvider);
  }

  @Override
  public void onConnectorEvent(SingleStorePartition partition, ConnectorEvent event) {
    if (event instanceof ObserveStreamingStartedEvent) {
      connected(true);
    }
  }
}
