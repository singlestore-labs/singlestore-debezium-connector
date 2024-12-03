package com.singlestore.debezium;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.DRIVER_CONFIG_PREFIX;

import com.singlestore.debezium.SingleStoreConnectorConfig.VectorHandlingMode;
import com.singlestore.debezium.SingleStoreValueConverters.VectorMode;
import com.singlestore.debezium.util.Utils;
import com.singlestore.jdbc.DatabaseMetaData;
import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnId;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.util.Strings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link JdbcConnection} extension to be used with SingleStore
 */
public class SingleStoreConnection extends JdbcConnection {

  protected static final String URL_PATTERN = "jdbc:singlestore://${hostname}:${port}/?connectTimeout=${connectTimeout}";
  protected static final String URL_PATTERN_DATABASE = "jdbc:singlestore://${hostname}:${port}/${dbname}?connectTimeout=${connectTimeout}";
  private static final Logger LOGGER = LoggerFactory.getLogger(
      SingleStoreConnection.class);
  private static final String QUOTED_CHARACTER = "`";
  private final SingleStoreConnectionConfiguration connectionConfig;

  public SingleStoreConnection(SingleStoreConnectionConfiguration connectionConfig) {
    super(connectionConfig.jdbcConfig, connectionConfig.factory,
        SingleStoreConnection::validateServerVersion, QUOTED_CHARACTER, QUOTED_CHARACTER);
    this.connectionConfig = connectionConfig;
  }

  private static void validateServerVersion(Statement statement) throws SQLException {
    DatabaseMetaData metaData = (DatabaseMetaData) statement.getConnection().getMetaData();
    if (!metaData.getVersion().versionGreaterOrEqual(8, 7, 16)) {
      throw new SQLException("The lowest supported version of SingleStore is 8.7.16");
    }
  }

  public String generateObserveQuery(TableId table, List<String> offsets) {
    return observeQuery(null, Set.of(table), Optional.empty(), Optional.empty(),
        Optional.of(String.format("(%s)", offsets
            .stream()
            .map(o -> o == null ? "NULL" : "'" + o + "'")
            .collect(Collectors.joining(",")))), Optional.empty());
  }

  /**
   * Executes OBSERVE query for CDC output stream events.
   *
   * @param tableFilter       tables filter to observe
   * @param resultSetConsumer the consumer of the query results
   * @return this object for chaining methods together
   * @throws SQLException if there is an error connecting to the database or executing the
   *                      statements
   */
  public JdbcConnection observe(Set<TableId> tableFilter,
      ResultSetConsumer resultSetConsumer) throws SQLException {
    return observe(null, tableFilter, Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), resultSetConsumer);
  }

  /**
   * Executes OBSERVE query for CDC output stream events.
   *
   * @param fieldFilter       columns filter to observe
   * @param tableFilter       tables filter to observe
   * @param resultSetConsumer the consumer of the query results
   * @return this object for chaining methods together
   * @throws SQLException if there is an error connecting to the database or executing the
   *                      statements
   */
  public JdbcConnection observe(Set<ColumnId> fieldFilter, Set<TableId> tableFilter,
      ResultSetConsumer resultSetConsumer) throws SQLException {
    return observe(fieldFilter, tableFilter, Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), resultSetConsumer);
  }

  /**
   * Executes OBSERVE query for CDC output stream events.
   *
   * @param fieldFilter       columns filter to observe
   * @param tableFilter       tables filter to observe
   * @param format            output format(SQL | JSON)
   * @param outputConfig      FS <FsConfig> | S3 <S3Config> | GCS <GCSConfig>
   * @param offSetConfig      offset config (<offset> | NULL),+ // # of partitions
   * @param recordFilter      filter on record metadata or content
   * @param resultSetConsumer the consumer of the query results
   * @return this object for chaining methods together
   * @throws SQLException if there is an error connecting to the database or executing the
   *                      statements
   */
  public JdbcConnection observe(Set<ColumnId> fieldFilter, Set<TableId> tableFilter,
      Optional<OBSERVE_OUTPUT_FORMAT> format,
      Optional<String> outputConfig, Optional<String> offSetConfig, Optional<String> recordFilter,
      ResultSetConsumer resultSetConsumer) throws SQLException {
    final String query = observeQuery(fieldFilter, tableFilter, format, outputConfig, offSetConfig,
        recordFilter);
    return query(query, resultSetConsumer);
  }

  private String observeQuery(Set<ColumnId> fieldFilter, Set<TableId> tableFilter,
      Optional<OBSERVE_OUTPUT_FORMAT> format,
      Optional<String> outputConfig, Optional<String> offSetConfig, Optional<String> recordFilter) {
    StringBuilder query = new StringBuilder("OBSERVE ");
    if (fieldFilter != null && !fieldFilter.isEmpty()) {
      query.append(fieldFilter.stream().map(this::quotedColumnIdString)
          .collect(Collectors.joining(","))).append(" FROM ");
    } else {
      query.append("* FROM ");
    }
    if (tableFilter != null && !tableFilter.isEmpty()) {
      query.append(
          tableFilter.stream().map(this::quotedTableIdString).collect(Collectors.joining(",")));
    } else {
      query.append("*");
    }
    format.ifPresent(f -> query.append(" AS ").append(f.name()));
    outputConfig.ifPresent(c -> query.append(" INTO ").append(c));
    offSetConfig.ifPresent(o -> query.append(" BEGIN AT ").append(o));
    recordFilter.ifPresent(f -> query.append(" WHERE ").append(f));
    return query.toString();
  }

  private List<String> getOldestSnapshotBeginnings(int numPartitions) {
    List<String> res = new ArrayList<>(Collections.nCopies(numPartitions, null));

    try (
        Statement stmt = connection().createStatement();
        ResultSet rs = stmt.executeQuery(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.OBSERVE_DATABASE_OFFSETS WHERE DATABASE_NAME = %s AND OFFSET_TYPE = 'snapshot_begin'",
                Utils.escapeString(database())))
    ) {
      while (rs.next()) {
        int partition = rs.getInt("ORDINAL");
        String offset = Utils.bytesToHex(rs.getBytes("OFFSET"));

        if (res.get(partition) == null || res.get(partition).compareTo(offset) > 0) {
          res.set(partition, offset);
        }
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DebeziumException(e);
    }
    return res;
  }

  public List<String> getLogTails(int numPartitions) {
    List<String> res = new ArrayList<>(Collections.nCopies(numPartitions, null));

    try (
        Statement stmt = connection().createStatement();
        ResultSet rs = stmt.executeQuery(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.OBSERVE_DATABASE_OFFSETS WHERE DATABASE_NAME = %s AND OFFSET_TYPE = 'log_tail'",
                Utils.escapeString(database())))
    ) {
      while (rs.next()) {
        int partition = rs.getInt("ORDINAL");
        String offset = Utils.bytesToHex(rs.getBytes("OFFSET"));

        res.set(partition, offset);
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DebeziumException(e);
    }
    return res;
  }

  /**
   * Validate observable offset before streaming.
   *
   * @param offset to validate
   * @return true if streaming is possible for given offset, false otherwise
   */
  public boolean validateOffset(Set<TableId> tableFilter, SingleStorePartition partition,
      SingleStoreOffsetContext offset) {
    List<String> oldestSnapshotBeginnings = getOldestSnapshotBeginnings(offset.offsets().size());
    List<String> offsets = offset.offsets();

    for (int i = 0; i < offsets.size(); i++) {
      String partitionOffset = offsets.get(i);
      String oldestSnapshotBeginning = oldestSnapshotBeginnings.get(i);
      if (partitionOffset != null && oldestSnapshotBeginning != null) {
        if (oldestSnapshotBeginning.compareTo(partitionOffset) > 0) {
          // Offset is stale
          LOGGER.warn("Failed to validate offset {}", offsets);
          return false;
        }
      }
    }

    LOGGER.trace("Offset {} is successfully validated", offset);
    return true;
  }

  public SingleStoreConnectionConfiguration connectionConfig() {
    return connectionConfig;
  }

  public String connectionString() {
    return database() != null ? connectionString(URL_PATTERN_DATABASE)
        : connectionString(URL_PATTERN);
  }

  @Override
  public String quotedTableIdString(TableId tableId) {
    return tableId.toQuotedString('`');
  }

  public String quotedColumnIdString(ColumnId columnId) {
    String columnName = columnId.columnName();
    char quotingChar = '`';
    if (columnName != null) {
      if (columnName.isEmpty()) {
        columnName = String.valueOf(quotingChar) + quotingChar;
      } else if (columnName.charAt(0) != quotingChar
          && columnName.charAt(columnName.length() - 1) != quotingChar) {
        columnName = columnName.replace("" + quotingChar, "" + quotingChar + quotingChar);
        columnName = quotingChar + columnName + quotingChar;
      }
    }
    return quotedTableIdString(columnId.tableId()) + "." + columnName;
  }

  @Override
  protected String[] supportedTableTypes() {
    return new String[]{"TABLE"};
  }

  @Override
  protected List<String> readPrimaryKeyOrUniqueIndexNames(java.sql.DatabaseMetaData metadata,
      TableId id)
      throws SQLException {
    return readPrimaryKeyNames(metadata, id);
  }

  public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern,
      TableFilter tableFilter, ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc)
      throws SQLException {
    super.readSchema(tables, databaseCatalog, schemaNamePattern, tableFilter, columnFilter,
        removeTablesNotFoundInJdbc);

    // Update tyeExpression for VECTOR columns
    Map<TableId, List<String>> tablesWithVectorColumns = new HashMap<>();

    for (TableId tableId : tables.tableIds()) {
      Table t = tables.forTable(tableId);
      List<String> vectorColumns = t.columns().stream()
          .filter(c -> c.typeName().equals("VECTOR"))
          .map(Column::name)
          .collect(Collectors.toList());

      if (!vectorColumns.isEmpty()) {
        tablesWithVectorColumns.put(tableId, vectorColumns);
      }
    }

    Map<TableId, Map<String, String>> columnToTypeExpression = getColumnToTypeExpression(
        tablesWithVectorColumns);

    for (TableId tableId : columnToTypeExpression.keySet()) {
      tables.updateTable(tableId, table -> {
        TableEditor editor = table.edit();
        for (Column column : table.columns()) {
          if (column.typeName().equals("VECTOR")) {
            Column updated = column.edit()
                .type(column.typeName(), columnToTypeExpression.get(tableId).get(column.name()))
                .create();
            editor.updateColumn(updated);
          }
        }

        return editor.create();
      });
    }
  }

  private Map<TableId, Map<String, String>> getColumnToTypeExpression(
      Map<TableId, List<String>> tablesWithVectorColumns) {
    Map<TableId, Map<String, String>> res = new HashMap<>();

    String query = String.format(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM information_schema.columns WHERE %s",
        tablesWithVectorColumns.keySet().stream()
            .map(tableId -> String.format("(TABLE_SCHEMA = %s AND TABLE_NAME = %s AND %s)",
                Utils.escapeString(tableId.catalog()),
                Utils.escapeString(tableId.table()),
                String.format("(%s)",
                    tablesWithVectorColumns.get(tableId).stream()
                        .map(column -> String.format("COLUMN_NAME = %s",
                            Utils.escapeString(column)))
                        .collect(Collectors.joining(" OR "))
                )))
            .collect(Collectors.joining(" OR "))
    );

    try (
        Statement stmt = connection().createStatement();
        ResultSet rs = stmt.executeQuery(query)
    ) {
      while (rs.next()) {
        String database = rs.getString(1);
        String table = rs.getString(2);
        String column = rs.getString(3);
        String typeExpresion = rs.getString(4);

        TableId id = new TableId(database, null, table);
        if (!res.containsKey(id)) {
          res.put(id, new HashMap<>());
        }

        res.get(id).put(column, typeExpresion);
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DebeziumException(e);
    }

    return res;
  }

  public enum OBSERVE_OUTPUT_FORMAT {
    SQL, JSON
  }

  public static class SingleStoreConnectionConfiguration {

    private final JdbcConfiguration jdbcConfig;
    private final ConnectionFactory factory;
    private final Configuration config;

    public SingleStoreConnectionConfiguration(Configuration config) {
      this.config = config;
      final boolean useSSL = sslModeEnabled();
      final Configuration dbConfig = config
          .edit()
          .withDefault(SingleStoreConnectorConfig.PORT,
              SingleStoreConnectorConfig.PORT.defaultValue())
          .build()
          .subset(DATABASE_CONFIG_PREFIX, true)
          .merge(config.subset(DRIVER_CONFIG_PREFIX, true));

      final Configuration.Builder jdbcConfigBuilder = dbConfig
          .edit()
          .with("connectTimeout", Long.toString(getConnectionTimeout().toMillis()))
          .with("sslMode", sslMode().getValue())
          .with("defaultFetchSize", 1)
          .with("tinyInt1IsBit", "false")
          .with("enableExtendedDataTypes", "true")
          .with("vectorTypeOutputFormat", vectorMode() == VectorMode.STRING ? "JSON" : "BINARY")
          .with("connectionAttributes", String.format(
              "_connector_name:%s,_connector_version:%s,_product_version:%s",
              "SingleStore Debezium Connector", Module.version(), Module.debeziumVersion()))
          .without("parameters");
      if (useSSL) {
        if (!Strings.isNullOrBlank(sslTrustStore())) {
          jdbcConfigBuilder.with("trustStore", "file:" + sslTrustStore());
        }
        if (sslTrustStorePassword() != null) {
          jdbcConfigBuilder.with("trustStorePassword", String.valueOf(sslTrustStorePassword()));
        }
        if (!Strings.isNullOrBlank(sslKeyStore())) {
          jdbcConfigBuilder.with("keyStore", "file:" + sslKeyStore());
        }
        if (sslKeyStorePassword() != null) {
          jdbcConfigBuilder.with("keyStorePassword", String.valueOf(sslKeyStorePassword()));
        }
        if (!Strings.isNullOrBlank(sslServerCertificate())) {
          jdbcConfigBuilder.with("serverSslCert", "file:" + sslServerCertificate());
        }
      }
      driverParameters().forEach(jdbcConfigBuilder::with);
      this.jdbcConfig = JdbcConfiguration.adapt(jdbcConfigBuilder.build());
      factory = JdbcConnection.patternBasedFactory(
          databaseName() != null ? SingleStoreConnection.URL_PATTERN_DATABASE
              : SingleStoreConnection.URL_PATTERN,
          com.singlestore.jdbc.Driver.class.getName(),
          getClass().getClassLoader());
    }

    public JdbcConfiguration config() {
      return jdbcConfig;
    }

    public Configuration originalConfig() {
      return config;
    }

    public ConnectionFactory factory() {
      return factory;
    }

    public String username() {
      return config.getString(SingleStoreConnectorConfig.USER);
    }

    public String password() {
      return config.getString(SingleStoreConnectorConfig.PASSWORD);
    }

    public String hostname() {
      return config.getString(SingleStoreConnectorConfig.HOSTNAME);
    }

    public int port() {
      return config.getInteger(SingleStoreConnectorConfig.PORT);
    }

    public String databaseName() {
      return config.getString(SingleStoreConnectorConfig.DATABASE_NAME);
    }

    public SingleStoreConnectorConfig.SecureConnectionMode sslMode() {
      String mode = config.getString(SingleStoreConnectorConfig.SSL_MODE);
      return SingleStoreConnectorConfig.SecureConnectionMode.parse(mode);
    }

    public VectorMode vectorMode() {
      return VectorHandlingMode
          .parse(config.getString(SingleStoreConnectorConfig.VECTOR_HANDLING_MODE))
          .asVectorMode();
    }

    public boolean sslModeEnabled() {
      return sslMode() != SingleStoreConnectorConfig.SecureConnectionMode.DISABLE;
    }

    public String sslKeyStore() {
      return config.getString(SingleStoreConnectorConfig.SSL_KEYSTORE);
    }

    public char[] sslKeyStorePassword() {
      String password = config.getString(SingleStoreConnectorConfig.SSL_KEYSTORE_PASSWORD);
      return Strings.isNullOrBlank(password) ? null : password.toCharArray();
    }

    public String sslTrustStore() {
      return config.getString(SingleStoreConnectorConfig.SSL_TRUSTSTORE);
    }

    public char[] sslTrustStorePassword() {
      String password = config.getString(SingleStoreConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
      return Strings.isNullOrBlank(password) ? null : password.toCharArray();
    }

    public String sslServerCertificate() {
      return config.getString(SingleStoreConnectorConfig.SSL_SERVER_CERT);
    }

    public Duration getConnectionTimeout() {
      return Duration.ofMillis(config.getLong(SingleStoreConnectorConfig.CONNECTION_TIMEOUT_MS));
    }

    public Map<String, String> driverParameters() {
      final String driverParametersString = config
          .getString(SingleStoreConnectorConfig.DRIVER_PARAMETERS);
      return driverParametersString == null ? Collections.emptyMap() : Arrays.stream(
              driverParametersString.split(";"))
          .map(s -> s.split("=")).collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
    }

    public CommonConnectorConfig.EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode() {
      String mode = config.getString(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);
      return CommonConnectorConfig.EventProcessingFailureHandlingMode.parse(mode);
    }
  }
}
