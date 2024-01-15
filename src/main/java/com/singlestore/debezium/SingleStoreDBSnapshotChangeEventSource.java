package com.singlestore.debezium;

import com.singlestore.debezium.exception.WrongOffsetException;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleStoreDBSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<SingleStoreDBPartition, SingleStoreDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleStoreDBSnapshotChangeEventSource.class);

    private final Set<String> OFFSET_SET = new HashSet<>();
    private volatile boolean offsetIsWrong;
    private final SingleStoreDBConnectorConfig connectorConfig;
    private final SingleStoreDBConnection jdbcConnection;
    private final SingleStoreDBDatabaseSchema schema;
    private final SnapshotProgressListener<SingleStoreDBPartition> snapshotProgressListener;
    private final MainConnectionProvidingConnectionFactory<? extends JdbcConnection> jdbcConnectionFactory;

    public SingleStoreDBSnapshotChangeEventSource(SingleStoreDBConnectorConfig connectorConfig,
                                                  MainConnectionProvidingConnectionFactory<SingleStoreDBConnection> jdbcConnectionFactory,
                                                  SingleStoreDBDatabaseSchema schema, EventDispatcher<SingleStoreDBPartition, TableId> dispatcher, Clock clock,
                                                  SnapshotProgressListener<SingleStoreDBPartition> snapshotProgressListener,
                                                  NotificationService<SingleStoreDBPartition, SingleStoreDBOffsetContext> notificationService) {
        super(connectorConfig, jdbcConnectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnectionFactory.mainConnection();
        this.schema = schema;
        this.snapshotProgressListener = snapshotProgressListener;
        this.jdbcConnectionFactory = jdbcConnectionFactory;
    }

    @Override
    public SnapshotResult<SingleStoreDBOffsetContext> doExecute(ChangeEventSourceContext context, SingleStoreDBOffsetContext previousOffset,
                                                                SnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext, SnapshottingTask snapshottingTask)
            throws Exception {
        final RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> ctx = (RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext>) snapshotContext;

        Connection connection = null;
        Exception exceptionWhileSnapshot = null;
        Queue<JdbcConnection> connectionPool = null;
        try {
            Set<Pattern> dataCollectionsToBeSnapshotted = getDataCollectionPattern(snapshottingTask.getDataCollections());

            LOGGER.info("Snapshot step 1 - Preparing");

            if (previousOffset != null && previousOffset.isSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            connection = createSnapshotConnection();
            connectionCreated(ctx);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            determineCapturedTables(ctx, dataCollectionsToBeSnapshotted);
            snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, ctx.capturedTables);

            LOGGER.info("Snapshot step 3 - Determining snapshot offset");
            determineSnapshotOffset(ctx, previousOffset);

            LOGGER.info("Snapshot step 4 - Reading structure of captured tables");
            readTableStructure(context, ctx, previousOffset, snapshottingTask);

            if (snapshottingTask.snapshotData()) {
                LOGGER.info("Snapshot step 4.a - Creating connection pool");
                connectionPool = createConnectionPool(ctx);
                LOGGER.info("Snapshot step 5 - Snapshotting data");
                createDataEvents(context, ctx, connectionPool);
            } else {
                LOGGER.info("Snapshot step 5 - Skipping snapshotting of data");
                releaseDataSnapshotLocks(ctx);
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
                                  RelationalSnapshotChangeEventSource.RelationalSnapshotContext<SingleStoreDBPartition,
                                          SingleStoreDBOffsetContext> snapshotContext,
                                  Queue<JdbcConnection> connectionPool) throws Exception {
        tryStartingSnapshot(snapshotContext);

        EventDispatcher.SnapshotReceiver<SingleStoreDBPartition> snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
        int snapshotMaxThreads = connectionPool.size();
        LOGGER.info("Creating snapshot with {} worker thread(s)", snapshotMaxThreads);
        ExecutorService executorService = Executors.newFixedThreadPool(snapshotMaxThreads);
        CompletionService<SingleStoreDBOffsetContext> completionService = new ExecutorCompletionService<>(executorService);
        Queue<SingleStoreDBOffsetContext> offsets = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < snapshotMaxThreads; i++) {
            offsets.add(copyOffset(snapshotContext));
        }

        Map<TableId, String> queryTables = new HashMap<>();
        Map<TableId, OptionalLong> rowCountTables = new LinkedHashMap<>();
        for (TableId tableId : snapshotContext.capturedTables) {
            final String selectStatement = determineSnapshotSelect(snapshotContext, tableId);
            LOGGER.info("For table '{}' using select statement: '{}'", tableId, selectStatement);
            queryTables.put(tableId, selectStatement);
            final OptionalLong rowCount = rowCountForTable(tableId);
            rowCountTables.put(tableId, rowCount);
        }

        int tableCount = rowCountTables.size();
        List<Callable<SingleStoreDBOffsetContext>> dataEventTasks = new ArrayList<>(tableCount);
        CyclicBarrier barrier = new CyclicBarrier(tableCount);
        int tableOrder = 1;
        for (TableId tableId : rowCountTables.keySet()) {
            boolean firstTable = tableOrder == 1 && snapshotMaxThreads == 1;
            boolean lastTable = tableOrder == tableCount && snapshotMaxThreads == 1;
            String selectStatement = queryTables.get(tableId);
            OptionalLong rowCount = rowCountTables.get(tableId);
            Callable<SingleStoreDBOffsetContext> callable = createDataEventsForTableCallable(sourceContext, snapshotContext, snapshotReceiver,
                    snapshotContext.tables.forTable(tableId), firstTable, lastTable, tableOrder++, tableCount, selectStatement,
                    rowCount, offsets, connectionPool, barrier);
            dataEventTasks.add(callable);
        }
        List<SingleStoreDBOffsetContext> commitSnapshotOffsetList = new ArrayList<>(tableCount);
        try {
            for (Callable<SingleStoreDBOffsetContext> callable : dataEventTasks) {
                completionService.submit(callable);
            }
            for (int i = 0; i < dataEventTasks.size(); i++) {
                commitSnapshotOffsetList.add(completionService.take().get());
            }
        } catch (ExecutionException e) {
            if (e.getCause() != null && e.getCause() instanceof WrongOffsetException) {
                throw new WrongOffsetException(e.getCause());
            } else {
                throw e;
            }
        } finally {
            offsetIsWrong = false;
            OFFSET_SET.clear();
            barrier.reset();
            executorService.shutdownNow();
        }
        commitSnapshotOffsetList.forEach(o -> {
            List<String> offsetList = o.offsets();
            for (int i = 0; i < offsetList.size(); i++) {
                if (offsetList.get(i) != null) {
                    snapshotContext.offset.update(i, o.txId(), offsetList.get(i));
                }
            }
        });

        for (SingleStoreDBOffsetContext offset : offsets) {
            offset.preSnapshotCompletion();
        }
        snapshotReceiver.completeSnapshot();
        for (SingleStoreDBOffsetContext offset : offsets) {
            offset.postSnapshotCompletion();
        }
    }

    private Callable<SingleStoreDBOffsetContext> createDataEventsForTableCallable(ChangeEventSource.ChangeEventSourceContext sourceContext, RelationalSnapshotChangeEventSource.RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext,
                                                                                  EventDispatcher.SnapshotReceiver<SingleStoreDBPartition> snapshotReceiver, Table table, boolean firstTable, boolean lastTable, int tableOrder,
                                                                                  int tableCount, String selectStatement, OptionalLong rowCount,
                                                                                  Queue<SingleStoreDBOffsetContext> offsets, Queue<JdbcConnection> connectionPool, CyclicBarrier barrier) {
        return () -> {
            JdbcConnection connection = connectionPool.poll();
            SingleStoreDBOffsetContext offset = offsets.poll();
            try {
                return doCreateDataEventsForTable(sourceContext, snapshotContext, offset, snapshotReceiver, table,
                        firstTable, lastTable, tableOrder, tableCount, selectStatement, rowCount, connection, barrier);
            } finally {
                offsets.add(offset);
                connectionPool.add(connection);
            }
        };
    }

    private SingleStoreDBOffsetContext doCreateDataEventsForTable(
            ChangeEventSource.ChangeEventSourceContext sourceContext,
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext,
            SingleStoreDBOffsetContext offset,
            EventDispatcher.SnapshotReceiver<SingleStoreDBPartition> snapshotReceiver, Table table,
            boolean firstTable, boolean lastTable, int tableOrder, int tableCount,
            String selectStatement, OptionalLong rowCount, JdbcConnection jdbcConnection, CyclicBarrier barrier)
            throws InterruptedException {
        SingleStoreDBPartition partition = snapshotContext.partition;
        if (!sourceContext.isRunning()) {
            throw new InterruptedException("Interrupted while snapshotting table " + table.id());
        }
        SingleStoreDBOffsetContext commitOffset = copyOffset(snapshotContext);
        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);
        Instant sourceTableSnapshotTimestamp = getSnapshotSourceTimestamp(jdbcConnection, offset, table.id());
        try (Statement statement = jdbcConnection.connection().createStatement();
             AutoClosableResultSetWrapper rsWrapper = AutoClosableResultSetWrapper.from(statement.executeQuery(selectStatement))) {
            ResultSet rs = rsWrapper.getResultSet();
            ObserveResultSetUtils.ColumnArray columnArray = 
                ObserveResultSetUtils.toArray(rs, table, schema.schemaFor(table.id()).valueSchema().fields());
            long rows = 0;
            Threads.Timer logTimer = getTableScanLogTimer();
            boolean hasNext = validateBeginSnapshotResultSet(rs);
            barrier.await();
            int numPartitions = snapshotContext.offset.offsets().size();
            if (hasNext) {
                while (hasNext && numPartitions > 0) {
                    if (offsetIsWrong) {
                        throw new InterruptedException("Interrupted while snapshotting table " + table.id() + ", because of wrong StartSnapshot offset");
                    }
                    if (ObserveResultSetUtils.isBeginSnapshot(rs)) {
                        hasNext = rs.next();
                    } else if (ObserveResultSetUtils.isCommitSnapshot(rs)) {
                        numPartitions--;
                        updateSnapshotOffset(commitOffset, rs);
                        if (numPartitions == 0) {
                            break;
                        }
                        hasNext = rs.next();
                    } else {
                        rows++;
                        final Object[] row = ObserveResultSetUtils.rowToArray(table, rs, columnArray);
                        final Long internalId = ObserveResultSetUtils.internalId(rs);
                        if (logTimer.expired()) {
                            long stop = clock.currentTimeInMillis();
                            if (rowCount.isPresent()) {
                                LOGGER.info("\t Exported {} of {} records for table '{}' after {}", rows, rowCount.getAsLong(),
                                        table.id(), Strings.duration(stop - exportStart));
                            } else {
                                LOGGER.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
                                        Strings.duration(stop - exportStart));
                            }
                            snapshotProgressListener.rowsScanned(partition, table.id(), rows);
                            logTimer = getTableScanLogTimer();
                        }
                        updateSnapshotOffset(offset, rs);
                        hasNext = rs.next();
                        setSnapshotMarker(offset, firstTable, lastTable, rows == 1, ObserveResultSetUtils.isCommitSnapshot(rs) && numPartitions == 1);
                        dispatcher.dispatchSnapshotEvent(partition, table.id(),
                                getChangeRecordEmitter(partition, offset, table.id(), row, internalId, sourceTableSnapshotTimestamp), snapshotReceiver);
                    }
                }
            } else {
                setSnapshotMarker(offset, firstTable, lastTable, false, true);
            }
            LOGGER.info("\t Finished exporting {} records for table '{}' ({} of {} tables); total duration '{}'",
                    rows, table.id(), tableOrder, tableCount, Strings.duration(clock.currentTimeInMillis() - exportStart));
            snapshotProgressListener.dataCollectionSnapshotCompleted(partition, table.id(), rows);
        } catch (SQLException | BrokenBarrierException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
        return commitOffset;
    }

    private boolean validateBeginSnapshotResultSet(ResultSet rs) throws SQLException {
        if (rs.next()) {
            if (!ObserveResultSetUtils.isBeginSnapshot(rs)) {
                LOGGER.warn("Observe query first row response must be of 'BeginSnapshot' type, skip snapshotting");
                return false;
            }
            String offset = ObserveResultSetUtils.offset(rs);
            validateBeginOffset(offset);
            return rs.next();
        }
        return false;
    }

    private synchronized void validateBeginOffset(String offset) {
        OFFSET_SET.add(offset);
        if (OFFSET_SET.size() > 1) {
            offsetIsWrong = true;
            throw new WrongOffsetException("StartSnapshot offset is wrong.");
        }
    }

    private void setSnapshotMarker(SingleStoreDBOffsetContext offset, boolean firstTable, boolean lastTable, boolean firstRecordInTable,
                                   boolean lastRecordInTable) {
        if (lastRecordInTable && lastTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST);
        } else if (firstRecordInTable && firstTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST);
        } else if (lastRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST_IN_DATA_COLLECTION);
        } else if (firstRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST_IN_DATA_COLLECTION);
        } else {
            offset.markSnapshotRecord(SnapshotRecord.TRUE);
        }
    }

    private void updateSnapshotOffset(SingleStoreDBOffsetContext offset, ResultSet rs) throws SQLException {
        final String offsetValue = ObserveResultSetUtils.offset(rs);
        final String txId = ObserveResultSetUtils.txId(rs);
        final Integer partitionId = ObserveResultSetUtils.partitionId(rs);
        offset.update(partitionId, txId, offsetValue);
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for the given table row.
     */
    protected ChangeRecordEmitter<SingleStoreDBPartition> getChangeRecordEmitter(SingleStoreDBPartition partition, SingleStoreDBOffsetContext offset, TableId tableId,
                                                                                 Object[] row, long internalId, Instant timestamp) {
        offset.event(tableId, timestamp);
        return new SingleStoreDBSnapshotChangeRecordEmitter(partition, offset, row, internalId, getClock(), connectorConfig);
    }

    private Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    private void determineCapturedTables(RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> ctx, Set<Pattern> dataCollectionsToBeSnapshotted) throws Exception {
        Set<TableId> allTableIds = getAllTableIds(ctx);
        Set<TableId> snapshottedTableIds = determineDataCollectionsToBeSnapshotted(allTableIds, dataCollectionsToBeSnapshotted).collect(Collectors.toSet());

        Set<TableId> capturedTables = new HashSet<>();
        Set<TableId> capturedSchemaTables = new HashSet<>();

        for (TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(tableId)) {
                LOGGER.info("Adding table {} to the list of capture schema tables", tableId);
                capturedSchemaTables.add(tableId);
            }
        }

        for (TableId tableId : snapshottedTableIds) {
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of captured tables for which the data will be snapshotted", tableId);
                capturedTables.add(tableId);
            } else {
                LOGGER.trace("Ignoring table {} for data snapshotting as it's not included in the filter configuration", tableId);
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
                .filter(tid -> pattern.asMatchPredicate().test(connectorConfig.getTableIdMapper().toString(tid)))
                .sorted();
    }

    private Queue<JdbcConnection> createConnectionPool(final RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> ctx) throws SQLException {
        Queue<JdbcConnection> connectionPool = new ConcurrentLinkedQueue<>();
        connectionPool.add(jdbcConnection);

        int snapshotMaxThreads = ctx.capturedTables.size();
        if (snapshotMaxThreads > 1) {
            Optional<String> firstQuery = getSnapshotConnectionFirstSelect(ctx, ctx.capturedTables.iterator().next());
            for (int i = 1; i < snapshotMaxThreads; i++) {
                JdbcConnection conn = jdbcConnectionFactory.newConnection().setAutoCommit(false);
                connectionPoolConnectionCreated(ctx, conn);
                connectionPool.add(conn);
                if (firstQuery.isPresent()) {
                    conn.execute(firstQuery.get());
                }
            }
        }
        LOGGER.info("Created connection pool with {} threads", snapshotMaxThreads);
        return connectionPool;
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
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{"TABLE"});
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext) {
    }

    @Override
    protected void determineSnapshotOffset(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> ctx,
            SingleStoreDBOffsetContext previousOffset) {
        if (previousOffset != null) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }
        ctx.offset = SingleStoreDBOffsetContext.initial(connectorConfig, () -> readNumberOfPartitions(ctx.catalogName));
    }

    private int readNumberOfPartitions(String database) {
        String query = "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = '" + database + "';";
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
                                      RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext,
                                      SingleStoreDBOffsetContext offsetContext,
                                      SnapshottingTask snapshottingTask) throws Exception {
        Set<String> catalogs = snapshotContext.capturedTables.stream()
                .map(TableId::catalog)
                .collect(Collectors.toSet());
        
        Tables.TableFilter tableFilter = snapshottingTask.isBlocking() ? Tables.TableFilter.fromPredicate(snapshotContext.capturedTables::contains)
            : connectorConfig.getTableFilters().dataCollectionFilter();

        for (String catalog : catalogs) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + catalog);
            }
            LOGGER.info("Reading structure of catalog '{}' of catalog '{}'", catalog, snapshotContext.catalogName);
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
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext)
            throws Exception {
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext, Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected SingleStoreDBOffsetContext copyOffset(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext) {
        return new SingleStoreDBOffsetContext.Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }

    /**
     * Returns a valid query string for the specified table, either given by the user via snapshot select overrides or
     * defaulting to a statement provided by the DB-specific change event source.
     *
     * @param tableId the table to generate a query for
     * @return a valid query string or empty if table will not be snapshotted
     */
    private String determineSnapshotSelect(RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext, TableId tableId) {
        List<String> columns = getPreparedColumnNames(snapshotContext.partition, schema.tableFor(tableId));
        return getSnapshotSelect(snapshotContext, tableId, columns).orElseThrow(() -> new IllegalArgumentException("Snapshot select query was not provided."));
    }

    @Override
    protected Optional<String> getSnapshotSelect(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext,
            TableId tableId, List<String> columns) {
        String snapshotSelectColumns = columns.stream()
                .collect(Collectors.joining(", "));//todo use in observe query
        return Optional.of(String.format("OBSERVE * FROM %s.%s", tableId.catalog(), tableId.table()));
    }

    @Override
    public SnapshottingTask getSnapshottingTask(SingleStoreDBPartition partition,
                                                   SingleStoreDBOffsetContext previousOffset) {
        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<String, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().identifier(), Map.Entry::getValue));

        boolean snapshotSchema = true;
        boolean snapshotData = true;
        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
            snapshotSchema = false;
            snapshotData = false;
        } else {
            LOGGER.info("No previous offset has been found");
            if (this.connectorConfig.getSnapshotMode().includeData()) {
                LOGGER.info("According to the connector configuration both schema and data will be snapshotted");
            } else {
                LOGGER.info("According to the connector configuration only schema will be snapshotted");
            }
            snapshotData = this.connectorConfig.getSnapshotMode().includeData();
        }
        return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable, false);
    }

    @Override
    protected RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> prepare(
            SingleStoreDBPartition partition) throws Exception {
        return new RelationalSnapshotContext<>(partition, connectorConfig.databaseName());
    }
}
