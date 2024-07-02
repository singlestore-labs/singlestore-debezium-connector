package com.singlestore.debezium;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.DRIVER_CONFIG_PREFIX;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.ColumnId;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * {@link JdbcConnection} extension to be used with SingleStore
 */
public class SingleStoreConnection extends JdbcConnection {

  private static final String QUOTED_CHARACTER = "`";
  protected static final String URL_PATTERN = "jdbc:singlestore://${hostname}:${port}/?connectTimeout=${connectTimeout}";
  protected static final String URL_PATTERN_DATABASE = "jdbc:singlestore://${hostname}:${port}/${dbname}?connectTimeout=${connectTimeout}";

  private final SingleStoreConnectionConfiguration connectionConfig;

  public SingleStoreConnection(SingleStoreConnectionConfiguration connectionConfig) {
    super(connectionConfig.jdbcConfig, connectionConfig.factory,
        SingleStoreConnection::validateServerVersion, QUOTED_CHARACTER, QUOTED_CHARACTER);
    this.connectionConfig = connectionConfig;
  }

  public boolean isRowstoreTable(TableId tableId) throws SQLException {
    AtomicBoolean res = new AtomicBoolean(false);
    prepareQuery(
        "SELECT table_type = 'INMEMORY_ROWSTORE' as isRowstore FROM information_schema.tables WHERE table_schema = ? && table_name = ?",
        statement -> {
          statement.setString(1, tableId.catalog());
          statement.setString(2, tableId.table());
        },
        rs -> {
          res.set(rs.getBoolean(1));
        });
    return res.get();
  }

  private static void validateServerVersion(Statement statement) throws SQLException {
    DatabaseMetaData metaData = statement.getConnection().getMetaData();
    int majorVersion = metaData.getDatabaseMajorVersion();
    int minorVersion = metaData.getDatabaseMinorVersion();
    if (majorVersion < 8 || (majorVersion == 8 && minorVersion < 5)) {
      throw new SQLException(
          "CDC feature is not supported in a version of SingleStore lower than 8.5");
    }
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
    return query(query.toString(), resultSetConsumer);
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
        columnName = new StringBuilder().append(quotingChar).append(quotingChar).toString();
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

  public enum OBSERVE_OUTPUT_FORMAT {
    SQL, JSON;
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
