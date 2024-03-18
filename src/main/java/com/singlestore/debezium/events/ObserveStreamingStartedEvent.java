package com.singlestore.debezium.events;

import io.debezium.pipeline.ConnectorEvent;

/**
 * An event that implies that a streaming observe is started.
 *
 */
public class ObserveStreamingStartedEvent implements ConnectorEvent {

  public static final ObserveStreamingStartedEvent INSTANCE = new ObserveStreamingStartedEvent();

  private ObserveStreamingStartedEvent() {
  }

}
