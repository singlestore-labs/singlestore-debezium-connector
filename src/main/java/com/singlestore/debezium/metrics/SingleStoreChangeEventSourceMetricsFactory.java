package com.singlestore.debezium.metrics;

import com.singlestore.debezium.SingleStorePartition;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class SingleStoreChangeEventSourceMetricsFactory extends
    DefaultChangeEventSourceMetricsFactory<SingleStorePartition> {

  private SingleStoreStreamingChangeEventSourceMetrics streamingMetrics;

  @Override
  public <T extends CdcSourceTaskContext> SingleStoreStreamingChangeEventSourceMetrics getStreamingMetrics(
      T taskContext,
      ChangeEventQueueMetrics changeEventQueueMetrics,
      EventMetadataProvider eventMetadataProvider) {
    if (streamingMetrics == null) {
      streamingMetrics = new SingleStoreStreamingChangeEventSourceMetrics(taskContext,
          changeEventQueueMetrics, eventMetadataProvider);
    }
    return streamingMetrics;
  }
}
