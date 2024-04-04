package com.singlestore.debezium;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

public class SingleStoreEventMetadataProvider implements EventMetadataProvider {

  @Override
  public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key,
      Struct value) {
    if (value == null) {
      return null;
    }
    final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
    if (source == null) {
      return null;
    }

    final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
    return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
  }

  @Override
  public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset,
      Object key,
      Struct value) {
    if (value == null) {
      return null;
    }
    final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
    if (source == null) {
      return null;
    }

    List<String> offsets = sourceInfo.<String>getArray(SourceInfo.OFFSETS_KEY);
    String offsetsString = offsets.stream().collect(Collectors.joining(","));

    return Collect.hashMapOf(SourceInfo.OFFSETS_KEY, offsetsString);
  }

  @Override
  public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key,
      Struct value) {
    if (value == null) {
      return null;
    }
    final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
    if (source == null) {
      return null;
    }

    return sourceInfo.getString(SourceInfo.TXID_KEY);
  }

}
