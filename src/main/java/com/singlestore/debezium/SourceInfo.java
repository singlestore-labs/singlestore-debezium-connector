package com.singlestore.debezium;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;


/**
 * Information about the source of information, which includes the partitions and offsets within those partitions.
 * 
 * <p>
 * The {@link SingleStoreDBPartition#getSourcePartition() source partition} information describes the cluster whose events are being consumed. 
 * Typically, the clutser is identified by the host address and the port number. Here's a JSON-like
 * representation of an example cluster:
 *
 * <pre>
 * {
 *     "server" : "production-server"
 * }
 * </pre>
 * 
 * The {@link SingleStoreDBOffsetContext#getOffset() source offset} information describes a structure containing the position in the server's offset for any
 * particular event for particular partition and transaction id. When performing snapshots, it may also contain a snapshot field which indicates that a particular record
 * is created while a snapshot it taking place.
 * Here's a JSON-like representation of an example:
 *
 * <pre>
 * {
 *     "partitionId" : 2,
 *     "txId" : "123",
 *     "offsets": ["23", null, "90", "54"],
 *     "snapshot": true
 * }
 * </pre>
 * <p>
 * The "{@code partitionId}" field describes partition index in which the event occured.
 * <p>
 * The "{@code txId}" field identifies database transaction in which the event occured.
 * <p>
 * The "{@code offsets}" is an array of offsets for each dataabase partition.
 * We need to save all offsets to be able to continue streaming after the connector is stopped 
 * using only information from the last record. 
 * <p>
 * 
 * The {@link #struct() source} struct appears in each message envelope and contains information about the event. It is
 * a mixture the fields from the {@link SingleStoreDBPartition#getSourcePartition() partition} and {@link SingleStoreDBPartition#getSourcePartition() offset}.
 * Like with the offset, the "{@code snapshot}" field only appears for events produced when the connector is in the
 * middle of a snapshot. Here's a JSON-like representation of the source for an event that corresponds to the above partition and
 * offset:
 * 
 * <pre>
 * {
 *     "name": "production-server",
 *     "partitionId" : 2,
 *     "txId" : "123",
 *     "offsets": ["23", null, "90", "54"],
 *     "snapshot": true
 * }
 * </pre>
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    public static final String TXID_KEY = "txId";
    public static final String PARTITIONID_KEY = "partitionId";
    public static final String OFFSETS_KEY = "offsets";

    private Integer partitionId;
    private String txId;
    private TableId tableId;
    private List<String> offsets;
    private Instant timestamp;

    
    public SourceInfo(SingleStoreDBConnectorConfig connectorConfig, Integer numPartitions) {
        super(connectorConfig);

        offsets = Collections.nCopies(numPartitions, null);
    }

    /**
     * Updates the source with information about a particular received or read event.
     *
     * @param partitionId index of the SingleStoreDB partition
     * @param txId the ID of the transaction that generated the transaction
     * @param offsets hex strings that represent offset for each database partition
     * @return this instance
     */
    protected SourceInfo update(Integer partitionId, String txId, List<String> offsets) {
        this.partitionId = partitionId;
        this.txId = txId;
        this.offsets = offsets;

        return this;
    }

    /**
     * Updates the source with information about a particular received or read event.
     *
     * @param partitionId index of the SingleStoreDB partition
     * @param txId the ID of the transaction that generated the transaction
     * @param offset hex strings that represent offset for given database partition
     * @return this instance
     */
    protected SourceInfo update(Integer partitionId, String txId, String offset) {
        this.partitionId = partitionId;
        this.txId = txId;
        this.offsets.set(partitionId, offset);

        return this;
    }

    /**
     * Updates the source with information about a table event.
     *
     * @param tableId table that was modified 
     * @param timestamp time when Debezium received the event information
     * @return this instance
     */
    protected SourceInfo update(TableId tableId, Instant timestamp) {
        this.tableId = tableId;
        this.timestamp = timestamp;

        return this;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    @Override
    protected String database() {
        return tableId == null ? "" : tableId.catalog();
    }

    protected String table() {
        return tableId == null ? "" : tableId.table();
    }

    protected String txId() {
        return txId;
    }

    protected Integer partitionId() {
        return partitionId;
    }

    protected List<String> offsets() {
        return offsets;
    }

    @Override
    public String toString() {
        return "SourceInfo [" +
                "serverName=" + serverName() +
                ", db=" + database() +
                ", table=" + table() +
                ", snapshot=" + snapshotRecord +
                ", partition=" + partitionId +
                ", transaction=" + txId +
                ", offsets=" + offsets +
                "]";
    }
}
