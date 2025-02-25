package com.singlestore.debezium;

import com.singlestore.debezium.util.ObserveResultSetUtils;
import io.debezium.connector.SnapshotRecord;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleStoreSnapshotChangeEventSource extends
    RelationalSnapshotChangeEventSource<SingleStorePartition, SingleStoreOffsetContext> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SingleStoreSnapshotChangeEventSource.class);

  private final SingleStoreConnectorConfig connectorConfig;
  private final SingleStoreConnection jdbcConnection;
  private final SingleStoreDatabaseSchema schema;
  private final SnapshotProgressListener<SingleStorePartition> snapshotProgressListener;

  public SingleStoreSnapshotChangeEventSource(SingleStoreConnectorConfig connectorConfig,
      MainConnectionProvidingConnectionFactory<SingleStoreConnection> jdbcConnectionFactory,
      SingleStoreDatabaseSchema schema, EventDispatcher<SingleStorePartition, TableId> dispatcher,
      Clock clock,
      SnapshotProgressListener<SingleStorePartition> snapshotProgressListener,
      NotificationService<SingleStorePartition, SingleStoreOffsetContext> notificationService) {
    super(connectorConfig, jdbcConnectionFactory, schema, dispatcher, clock,
        snapshotProgressListener, notificationService);
    this.connectorConfig = connectorConfig;
    this.jdbcConnection = jdbcConnectionFactory.mainConnection();
    this.schema = schema;
    this.snapshotProgressListener = snapshotProgressListener;
  }

  @Override
  public SnapshotResult<SingleStoreOffsetContext> doExecute(ChangeEventSourceContext context,
      SingleStoreOffsetContext previousOffset,
      SnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext,
      SnapshottingTask snapshottingTask)
      throws Exception {
    final RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> ctx = (RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext>) snapshotContext;

    Connection connection = null;
    Exception exceptionWhileSnapshot = null;
    try {
      Set<Pattern> dataCollectionsToBeSnapshotted = getDataCollectionPattern(
          snapshottingTask.getDataCollections());

      LOGGER.info("Snapshot step 1 - Preparing");

      if (previousOffset != null && previousOffset.isSnapshotRunning()) {
        LOGGER.info(
            "Previous snapshot was cancelled before completion; a new snapshot will be taken.");
      }

      connection = createSnapshotConnection();
      connectionCreated(ctx);

      LOGGER.info("Snapshot step 2 - Determining captured tables");

      // Note that there's a minor race condition here: a new table matching the filters could be created between
      // this call and the determination of the initial snapshot position below; this seems acceptable, though
      determineCapturedTables(ctx, dataCollectionsToBeSnapshotted);
      snapshotProgressListener
          .monitoredDataCollectionsDetermined(snapshotContext.partition, ctx.capturedTables);

      LOGGER.info("Snapshot step 3 - Determining snapshot offset");
      determineSnapshotOffset(ctx, previousOffset);

      LOGGER.info("Snapshot step 4 - Reading structure of captured tables");
      readTableStructure(context, ctx, previousOffset, snapshottingTask);

      if (snapshottingTask.snapshotData()) {
        LOGGER.info("Snapshot step 4.a - Creating connection pool");
        LOGGER.info("Snapshot step 5 - Snapshotting data");
        createDataEvents(context, ctx, jdbcConnection);
      } else {
        LOGGER.info("Snapshot step 5 - Skipping snapshotting of data");
        releaseDataSnapshotLocks(ctx);

        if (connectorConfig.offsets() != null && !connectorConfig.offsets().isEmpty()) {
          for (int i = 0; i < connectorConfig.offsets().size(); i++) {
            String offset = connectorConfig.offsets().get(i);
            if (!offset.equalsIgnoreCase("null")) {
              ctx.offset.update(i, "", offset);
            }
          }
        } else if (ctx.offset.offsets().stream().allMatch(Objects::isNull)) {
          // We are intended to skip snapshotting of data,
          // and we don't have any previous offset.
          // Retrieve log tail and start streaming from it.
          List<String> offsets = this.jdbcConnection.getLogTails(
              snapshotContext.offset.offsets().size());
          for (int i = 0; i < offsets.size(); i++) {
            String offset = offsets.get(i);
            if (!offset.equalsIgnoreCase("null")) {
              ctx.offset.update(i, "", offset);
            }
          }
        }

        ctx.offset.preSnapshotCompletion();
        ctx.offset.postSnapshotCompletion();
      }

      postSnapshot();
      dispatcher.alwaysDispatchHeartbeatEvent(ctx.partition, ctx.offset);
      return SnapshotResult.completed(ctx.offset);
    } catch (final Exception e) {
      LOGGER.error("Error during snapshot", e);
      exceptionWhileSnapshot = e;
      throw e;
    } finally {
      try {
        rollbackTransaction(connection);
      } catch (final Exception e) {
        LOGGER.error("Error in finally block", e);
        if (exceptionWhileSnapshot != null) {
          e.addSuppressed(exceptionWhileSnapshot);
        }
        throw e;
      }
    }
  }

  private void createDataEvents(ChangeEventSource.ChangeEventSourceContext sourceContext,
      RelationalSnapshotChangeEventSource.RelationalSnapshotContext<SingleStorePartition,
          SingleStoreOffsetContext> snapshotContext,
      JdbcConnection conn) throws Exception {
    tryStartingSnapshot(snapshotContext);

    EventDispatcher.SnapshotReceiver<SingleStorePartition> snapshotReceiver = dispatcher
        .getSnapshotChangeEventReceiver();

    // We must have only one table
    assert (snapshotContext.capturedTables.size() == 1);
    TableId table = snapshotContext.capturedTables.iterator().next();
    final String selectStatement = determineSnapshotSelect(snapshotContext, table);

    if (!snapshotContext.offset.isSnapshotRunning()) {
      snapshotContext.offset.preSnapshotStart();
    }

    doCreateDataEventsForTable(sourceContext, snapshotContext, snapshotContext.offset,
        snapshotReceiver, snapshotContext.tables.forTable(table), selectStatement,
        conn);

    snapshotContext.offset.preSnapshotCompletion();
    snapshotReceiver.completeSnapshot();
    snapshotContext.offset.postSnapshotCompletion();
  }

  private void doCreateDataEventsForTable(
      ChangeEventSource.ChangeEventSourceContext sourceContext,
      RelationalSnapshotChangeEventSource.RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext,
      SingleStoreOffsetContext offset,
      EventDispatcher.SnapshotReceiver<SingleStorePartition> snapshotReceiver, Table table,
      String selectStatement, JdbcConnection jdbcConnection)
      throws InterruptedException {
    SingleStorePartition partition = snapshotContext.partition;
    if (!sourceContext.isRunning()) {
      throw new InterruptedException("Interrupted while snapshotting table " + table.id());
    }
    long exportStart = clock.currentTimeInMillis();
    LOGGER.info("Exporting data from table '{}'", table.id());
    Instant sourceTableSnapshotTimestamp = getSnapshotSourceTimestamp(jdbcConnection, offset,
        table.id());
    try (Statement statement = jdbcConnection.connection().createStatement();
        AutoClosableResultSetWrapper rsWrapper = AutoClosableResultSetWrapper
            .from(statement.executeQuery(selectStatement))) {
      ResultSet rs = rsWrapper.getResultSet();
      List<Integer> columnPostitions =
          ObserveResultSetUtils
              .columnPositions(rs, table.columns());
      long rows = 0;
      Threads.Timer logTimer = getTableScanLogTimer();

      int numPartitions = snapshotContext.offset.offsets().size();
      List<Boolean> snapshotCommitted = new ArrayList<>(Collections.nCopies(numPartitions, false));
      while (numPartitions > 0 && rs.next()) {
        LOGGER.trace(
            "Snapshot record, type: {}, internalId: {}, partitionId: {}, offset: {} values: {}",
            ObserveResultSetUtils.snapshotType(rs),
            ObserveResultSetUtils.internalId(rs),
            ObserveResultSetUtils.partitionId(rs),
            ObserveResultSetUtils.offset(rs),
            ObserveResultSetUtils.rowToArray(rs, columnPostitions,
                connectorConfig.populateInternalId(), connectorConfig.getVectorMode()));

        int partitionId = ObserveResultSetUtils.partitionId(rs);
        if (ObserveResultSetUtils.isCommitSnapshot(rs)) {
          numPartitions--;
          snapshotCommitted.set(partitionId, true);
        } else if (!ObserveResultSetUtils.isBeginSnapshot(rs)
            && !snapshotCommitted.get(partitionId)) {
          rows++;
          final Object[] row = ObserveResultSetUtils.rowToArray(rs, columnPostitions,
              connectorConfig.populateInternalId(), connectorConfig.getVectorMode());
          final String internalId = ObserveResultSetUtils.internalId(rs);
          if (logTimer.expired()) {
            long stop = clock.currentTimeInMillis();
            LOGGER.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
                Strings.duration(stop - exportStart));
            snapshotProgressListener.rowsScanned(partition, table.id(), rows);
            logTimer = getTableScanLogTimer();
          }
          updateSnapshotOffset(offset, rs);
          if (rows == 1) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST);
          }
          dispatcher.dispatchSnapshotEvent(partition, table.id(),
              getChangeRecordEmitter(partition, offset, table.id(), table, row,
                  internalId,
                  sourceTableSnapshotTimestamp), snapshotReceiver);
        }
      }

      LOGGER.info(
          "\t Finished exporting {} records for table '{}'; total duration '{}'",
          rows, table.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
      snapshotProgressListener.dataCollectionSnapshotCompleted(partition, table.id(), rows);
    } catch (SQLException e) {
      SQLException error = e;
      if (StaleOffsetException.isStaleOffsetException(error)) {
        error = new StaleOffsetException(error);
      }

      throw new ConnectException("Snapshotting of table " + table.id() + " failed", error);
    }
  }

  private void updateSnapshotOffset(SingleStoreOffsetContext offset, ResultSet rs)
      throws SQLException {
    final String offsetValue = ObserveResultSetUtils.offset(rs);
    final String txId = ObserveResultSetUtils.txId(rs);
    final Integer partitionId = ObserveResultSetUtils.partitionId(rs);
    offset.update(partitionId, txId, offsetValue);
  }

  /**
   * Returns a {@link ChangeRecordEmitter} producing the change records for the given table row.
   */
  protected ChangeRecordEmitter<SingleStorePartition> getChangeRecordEmitter(
      SingleStorePartition partition, SingleStoreOffsetContext offset, TableId tableId, Table table,
      Object[] row, String internalId, Instant timestamp) {
    offset.event(tableId, timestamp);
    return new SingleStoreSnapshotChangeRecordEmitter(partition, offset, row, internalId,
        getClock(), connectorConfig, table);
  }

  private Threads.Timer getTableScanLogTimer() {
    return Threads.timer(clock, LOG_INTERVAL);
  }

  private void determineCapturedTables(
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> ctx,
      Set<Pattern> dataCollectionsToBeSnapshotted) throws Exception {
    Set<TableId> allTableIds = getAllTableIds(ctx);
    Set<TableId> snapshottedTableIds = determineDataCollectionsToBeSnapshotted(allTableIds,
        dataCollectionsToBeSnapshotted).collect(Collectors.toSet());

    Set<TableId> capturedTables = new HashSet<>();
    Set<TableId> capturedSchemaTables = new HashSet<>();

    for (TableId tableId : allTableIds) {
      if (connectorConfig.getTableFilters().eligibleForSchemaDataCollectionFilter()
          .isIncluded(tableId)) {
        LOGGER.info("Adding table {} to the list of capture schema tables", tableId);
        capturedSchemaTables.add(tableId);
      }
    }

    for (TableId tableId : snapshottedTableIds) {
      if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
        LOGGER.trace(
            "Adding table {} to the list of captured tables for which the data will be snapshotted",
            tableId);
        capturedTables.add(tableId);
      } else {
        LOGGER.trace(
            "Ignoring table {} for data snapshotting as it's not included in the filter configuration",
            tableId);
      }
    }

    ctx.capturedTables = addSignalingCollectionAndSort(capturedTables);
    ctx.capturedSchemaTables = capturedSchemaTables
        .stream()
        .sorted()
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private Set<TableId> addSignalingCollectionAndSort(Set<TableId> capturedTables) throws Exception {
    String tableIncludeList = connectorConfig.tableIncludeList();
    String signalingDataCollection = connectorConfig.getSignalingDataCollectionId();
    List<Pattern> captureTablePatterns = new ArrayList<>();
    if (!Strings.isNullOrBlank(tableIncludeList)) {
      captureTablePatterns.addAll(Strings.listOfRegex(tableIncludeList, Pattern.CASE_INSENSITIVE));
    }
    if (!Strings.isNullOrBlank(signalingDataCollection)) {
      captureTablePatterns.addAll(getSignalDataCollectionPattern(signalingDataCollection));
    }
    if (captureTablePatterns.size() > 0) {
      return captureTablePatterns
          .stream()
          .flatMap(pattern -> toTableIds(capturedTables, pattern))
          .collect(Collectors.toCollection(LinkedHashSet::new));
    }
    return capturedTables
        .stream()
        .sorted()
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private Stream<TableId> toTableIds(Set<TableId> tableIds, Pattern pattern) {
    return tableIds
        .stream()
        .filter(tid -> pattern.asMatchPredicate()
            .test(connectorConfig.getTableIdMapper().toString(tid)))
        .sorted();
  }

  private void rollbackTransaction(Connection connection) {
    if (connection != null) {
      try {
        connection.rollback();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected Set<TableId> getAllTableIds(
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> ctx)
      throws Exception {
    return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{"TABLE"});
  }

  @Override
  protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext) {
  }

  @Override
  protected void determineSnapshotOffset(
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> ctx,
      SingleStoreOffsetContext previousOffset) {
    if (previousOffset != null) {
      ctx.offset = previousOffset;
      tryStartingSnapshot(ctx);
      return;
    }
    ctx.offset = SingleStoreOffsetContext
        .initial(connectorConfig, () -> readNumberOfPartitions(ctx.catalogName));
  }

  private int readNumberOfPartitions(String database) {
    String query =
        "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = '"
            + database + "';";
    try (Statement statement = jdbcConnection.connection().createStatement();
        ResultSet rs = statement.executeQuery(query)) {
      if (rs.next()) {
        return rs.getInt(1);
      }
    } catch (SQLException e) {
      LOGGER.warn("Failed to read number of partitions for database '" + database + "'.");
    }
    return 1;
  }

  @Override
  protected void readTableStructure(ChangeEventSourceContext sourceContext,
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext,
      SingleStoreOffsetContext offsetContext,
      SnapshottingTask snapshottingTask) throws Exception {
    Set<String> catalogs = snapshotContext.capturedTables.stream()
        .map(TableId::catalog)
        .collect(Collectors.toSet());

    Tables.TableFilter tableFilter = snapshottingTask.isOnDemand() ? Tables.TableFilter
        .fromPredicate(snapshotContext.capturedTables::contains)
        : connectorConfig.getTableFilters().dataCollectionFilter();

    for (String catalog : catalogs) {
      if (!sourceContext.isRunning()) {
        throw new InterruptedException("Interrupted while reading structure of schema " + catalog);
      }
      LOGGER.info("Reading structure of catalog '{}' of catalog '{}'", catalog,
          snapshotContext.catalogName);
      jdbcConnection.readSchema(
          snapshotContext.tables,
          catalog,
          null,
          tableFilter,
          null,
          false);
    }
    schema.refresh(jdbcConnection);
  }

  @Override
  protected void releaseSchemaSnapshotLocks(
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext)
      throws Exception {
  }

  @Override
  protected SchemaChangeEvent getCreateTableEvent(
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext,
      Table table) {
    return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset,
        snapshotContext.catalogName, table);
  }

  @Override
  protected SingleStoreOffsetContext copyOffset(
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext) {
    return new SingleStoreOffsetContext.Loader(connectorConfig)
        .load(snapshotContext.offset.getOffset());
  }

  /**
   * Returns a valid query string for the specified table, either given by the user via snapshot
   * select overrides or defaulting to a statement provided by the DB-specific change event source.
   *
   * @param tableId the table to generate a query for
   * @return a valid query string or empty if table will not be snapshotted
   */
  private String determineSnapshotSelect(
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext,
      TableId tableId) {
    List<String> columns = getPreparedColumnNames(snapshotContext.partition,
        schema.tableFor(tableId));
    return getSnapshotSelect(snapshotContext, tableId, columns)
        .orElseThrow(() -> new IllegalArgumentException("Snapshot select query was not provided."));
  }

  @Override
  protected Optional<String> getSnapshotSelect(
      RelationalSnapshotContext<SingleStorePartition, SingleStoreOffsetContext> snapshotContext,
      TableId tableId, List<String> columns) {
    String snapshotSelectColumns = columns.stream()
        .collect(Collectors.joining(", "));//todo use in observe query
    return Optional.of(
        jdbcConnection.generateObserveQuery(tableId, snapshotContext.offset.offsets()));
  }

  @Override
  public SnapshottingTask getSnapshottingTask(SingleStorePartition partition,
      SingleStoreOffsetContext previousOffset) {
    List<String> dataCollectionsToBeSnapshotted = connectorConfig
        .getDataCollectionsToBeSnapshotted();
    Map<String, String> snapshotSelectOverridesByTable = connectorConfig
        .getSnapshotSelectOverridesByTable().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().identifier(), Map.Entry::getValue));

    boolean snapshotSchema = true;
    boolean snapshotData = true;
    // found a previous offset and the earlier snapshot has completed
    if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
      LOGGER.info(
          "A previous offset indicating a completed snapshot has been found. Only schema will be snapshotted.");
      snapshotData = false;
    } else {
      LOGGER.info("No previous offset has been found");
      if (this.connectorConfig.getSnapshotMode().includeData()) {
        LOGGER.info(
            "According to the connector configuration both schema and data will be snapshotted");
      } else {
        LOGGER.info("According to the connector configuration only schema will be snapshotted");
      }
      snapshotData = this.connectorConfig.getSnapshotMode().includeData();
    }
    return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted,
        snapshotSelectOverridesByTable, false);
  }

  @Override
  protected SnapshotContext<SingleStorePartition, SingleStoreOffsetContext> prepare(
      SingleStorePartition singleStorePartition, boolean onDemand) {
    return new RelationalSnapshotContext<>(singleStorePartition, connectorConfig.databaseName(),
        onDemand);
  }
}
