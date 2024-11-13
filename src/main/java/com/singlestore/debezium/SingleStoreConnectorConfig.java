package com.singlestore.debezium;

import com.singlestore.debezium.SingleStoreValueConverters.GeographyMode;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DefaultTopicNamingStrategy;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

/**
 * The configuration properties.
 */
public class SingleStoreConnectorConfig extends RelationalDatabaseConnectorConfig {

  public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
      .withDefault(SingleStoreSourceInfoStructMaker.class.getName());
  public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
      .withDisplayName("Snapshot mode")
      .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
      .withWidth(ConfigDef.Width.SHORT)
      .withImportance(ConfigDef.Importance.LOW)
      .withDescription("The criteria for running a snapshot upon startup of the connector. "
          + "Select one of the following snapshot options: "
          + "'initial' (default): If the connector does not detect any offsets for the logical server name, it performs a full snapshot that captures the current state of the configured tables. After the snapshot completes, the connector begins to stream changes.; "
          + "'initial_only': Similar to the 'initial' mode, the connector performs a full snapshot. Once the snapshot is complete, the connector stops, and does not stream any changes.");
  public static final Field CONNECTION_TIMEOUT_MS = Field.create("connect.timeout.ms")
      .withDisplayName("Connection Timeout (ms)")
      .withType(ConfigDef.Type.INT)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
      .withWidth(ConfigDef.Width.SHORT)
      .withImportance(ConfigDef.Importance.MEDIUM)
      .withDescription(
          "Maximum time to wait after trying to connect to the database before timing out, given in milliseconds. Defaults to 30 seconds (30,000 ms).")
      .withDefault(30 * 1000)
      .withValidation(Field::isPositiveInteger);
  public static final Field DRIVER_PARAMETERS = Field.create(DRIVER_CONFIG_PREFIX + "parameters")
      .withDisplayName("JDBC Additional Parameters")
      .withType(ConfigDef.Type.STRING)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 2))
      .withWidth(ConfigDef.Width.LONG)
      .withImportance(ConfigDef.Importance.MEDIUM)
      .withDescription(
          "Additional JDBC parameters to use with connection string to SingleStore server. Format: 'param1=value1; param2 = value2; ...'. The supported parameters are\n"
              +
              "available in the `SingleStore Connection String Parameters\n" +
              "<https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters>`_.");
  public static final Field SSL_MODE = Field.create("database.ssl.mode")
      .withDisplayName("SSL mode")
      .withEnum(SecureConnectionMode.class, SecureConnectionMode.DISABLE)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 0))
      .withWidth(ConfigDef.Width.MEDIUM)
      .withImportance(ConfigDef.Importance.MEDIUM)
      .withDescription("Whether to use an encrypted connection to SingleStore. Options include: "
          + "'disable' to use an unencrypted connection (the default); "
          + "'trust' to use a secure (encrypted) connection (no certificate and hostname validation); "
          + "'verify_ca' to use a secure (encrypted) connection but additionally verify the server TLS certificate against the configured Certificate Authority "
          + "(CA) certificates, or fail if no valid matching CA certificates are found; or"
          + "'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.");
  public static final Field SSL_KEYSTORE = Field.create("database.ssl.keystore")
      .withDisplayName("SSL Keystore")
      .withType(ConfigDef.Type.STRING)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 1))
      .withWidth(ConfigDef.Width.LONG)
      .withImportance(ConfigDef.Importance.MEDIUM)
      .withDescription("The location of the key store file. "
          + "This is optional and can be used for two-way authentication between the client and the SingleStore Server.");
  public static final Field SSL_KEYSTORE_PASSWORD = Field.create("database.ssl.keystore.password")
      .withDisplayName("SSL Keystore Password")
      .withType(ConfigDef.Type.PASSWORD)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 2))
      .withWidth(ConfigDef.Width.MEDIUM)
      .withImportance(ConfigDef.Importance.MEDIUM)
      .withDescription("The password for the key store file. "
          + "This is optional and only needed if 'database.ssl.keystore' is configured.");
  public static final Field SSL_TRUSTSTORE = Field.create("database.ssl.truststore")
      .withDisplayName("SSL Truststore")
      .withType(ConfigDef.Type.STRING)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 3))
      .withWidth(ConfigDef.Width.LONG)
      .withImportance(ConfigDef.Importance.MEDIUM)
      .withDescription(
          "The location of the trust store file for the server certificate verification.");
  public static final Field SSL_TRUSTSTORE_PASSWORD = Field
      .create("database.ssl.truststore.password")
      .withDisplayName("SSL Truststore Password")
      .withType(ConfigDef.Type.PASSWORD)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 4))
      .withWidth(ConfigDef.Width.MEDIUM)
      .withImportance(ConfigDef.Importance.MEDIUM)
      .withDescription("The password for the trust store file. "
          + "Used to check the integrity of the truststore, and unlock the truststore.");
  public static final Field SSL_SERVER_CERT = Field.create("database.ssl.server.cert")
      .withDisplayName("SSL Server's Certificate")
      .withType(ConfigDef.Type.PASSWORD)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 5))
      .withWidth(ConfigDef.Width.MEDIUM)
      .withImportance(ConfigDef.Importance.MEDIUM)
      .withDescription("Server's certificate in DER format or the server's CA certificate. " +
          "The certificate is added to the trust store, which allows the connection to trust a self-signed certificate.");
  public static final Field TABLE_NAME = Field.create(DATABASE_CONFIG_PREFIX + "table")
      .withDisplayName("Table")
      .withType(ConfigDef.Type.STRING)
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 7))
      .withWidth(Width.MEDIUM)
      .withImportance(Importance.HIGH)
      .required()
      .withDescription("The name of the table from which the connector should capture changes");
  public static final Field POPULATE_INTERNAL_ID = Field.create("populate.internal.id")
      .withDisplayName("Add internalId to the `after` field of the event message")
      .withType(ConfigDef.Type.BOOLEAN)
      .withWidth(Width.SHORT)
      .withImportance(Importance.MEDIUM)
      .withDescription(
          "Specifies whether to add internalId to the `after` field of the event message")
      .withDefault(false);
  public static final Field OFFSETS = Field.create("offsets")
      .withDisplayName(
          "Offsets from which to start observing when 'snapshot.mode' is 'no_data'")
      .withType(Type.LIST)
      .withWidth(Width.LONG)
      .withImportance(Importance.LOW)
      .withDescription(
          "When specified and 'snapshot.mode' is 'no_data' - connector will start streaming changes from these offsets. "
              + "Should be provided as a comma separated list of hex offsets per each partitions. "
              + "Example: 0000000000000077000000000000000E000000000000E06E,0x0000000000000077000000000000000E000000000000E087,0000000000000077000000000000000E000000000000E088");
  public static final Field TOPIC_NAMING_STRATEGY = CommonConnectorConfig.TOPIC_NAMING_STRATEGY
      .withDefault(DefaultTopicNamingStrategy.class.getName());

  public static final Field GEOGRAPHY_HANDLING_MODE = Field.create("geography.handling.mode")
      .withDisplayName("Geography Handling")
      .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 8))
      .withEnum(GeographyHandlingMode.class, GeographyHandlingMode.GEOMETRY)
      .withWidth(Width.SHORT)
      .withImportance(Importance.MEDIUM)
      .withDescription(
          "Specify how GEOGRAPHY and GEOGRAPHYPOINT columns should be represented in change events, including: "
              + "'geometry' (the default) uses io.debezium.data.geometry.Geometry to represent values, which contains a structure with two fields: srid (INT32): spatial reference system ID that defines the type of geometry object stored in the structure and wkb (BYTES): binary representation of the geometry object encoded in the Well-Known-Binary (wkb) format."
              + "'string' uses string to represent values.");

  protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 10_240;
  protected static final int DEFAULT_PORT = 3306;
  public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
      .withDefault(DEFAULT_PORT);
  private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION
      .edit()
      .name("SingleStore")
      .excluding(SNAPSHOT_LOCK_TIMEOUT_MS,
          MSG_KEY_COLUMNS,
          INCLUDE_SCHEMA_COMMENTS,
          INCLUDE_SCHEMA_CHANGES,
          SCHEMA_INCLUDE_LIST,
          SCHEMA_EXCLUDE_LIST,
          QUERY_FETCH_SIZE,
          SNAPSHOT_FETCH_SIZE,
          SNAPSHOT_MAX_THREADS,
          TABLE_IGNORE_BUILTIN,
          SNAPSHOT_MODE_TABLES,
          TABLE_INCLUDE_LIST,
          TABLE_EXCLUDE_LIST,
          INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
          SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
          INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES,
          INCREMENTAL_SNAPSHOT_WATERMARKING_STRATEGY,
          SIGNAL_DATA_COLLECTION,
          SIGNAL_POLL_INTERVAL_MS,
          SIGNAL_ENABLED_CHANNELS,
          SNAPSHOT_FULL_COLUMN_SCAN_FORCE, //single table supported
          SNAPSHOT_TABLES_ORDER_BY_ROW_COUNT, //single table supported
          // TODO PLAT-6820 implement transaction monitoring
          PROVIDE_TRANSACTION_METADATA)
      .type(
          HOSTNAME,
          PORT,
          USER,
          PASSWORD,
          DATABASE_NAME,
          TABLE_NAME,
          SSL_MODE,
          SSL_KEYSTORE,
          SSL_KEYSTORE_PASSWORD,
          SSL_TRUSTSTORE,
          SSL_TRUSTSTORE_PASSWORD,
          SSL_SERVER_CERT)
      .connector(
          CONNECTION_TIMEOUT_MS,
          DRIVER_PARAMETERS,
          SNAPSHOT_MODE,
          BINARY_HANDLING_MODE,
          GEOGRAPHY_HANDLING_MODE,
          OFFSETS)
      .events(
          SOURCE_INFO_STRUCT_MAKER,
          POPULATE_INTERNAL_ID)
      .create();
  /**
   * The set of {@link Field}s defined as part of this configuration.
   */
  public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());
  private final Configuration config;
  private final SnapshotMode snapshotMode;
  private final Duration connectionTimeout;
  private final RelationalTableFilters tableFilters;
  private final Boolean populateInternalId;
  private final List<String> offsets;

  public SingleStoreConnectorConfig(Configuration config) {
    super(config,
        new SystemTablesPredicate(),
        t -> t.catalog() + "." + t.table(),
        DEFAULT_SNAPSHOT_FETCH_SIZE,
        ColumnFilterMode.CATALOG,
        false);
    this.config = config;
    this.tableFilters = new SingleStoreTableFilters(config, new SystemTablesPredicate(),
        getTableIdMapper(), false);
    this.snapshotMode = SnapshotMode
        .parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
    this.connectionTimeout = Duration
        .ofMillis(config.getLong(SingleStoreConnectorConfig.CONNECTION_TIMEOUT_MS));
    this.populateInternalId = config.getBoolean(SingleStoreConnectorConfig.POPULATE_INTERNAL_ID);
    this.offsets = config.getList(SingleStoreConnectorConfig.OFFSETS);
  }

  public static ConfigDef configDef() {
    return CONFIG_DEFINITION.configDef();
  }

  @Override
  public String getContextName() {
    return Module.contextName();
  }

  @Override
  public String getConnectorName() {
    return Module.name();
  }

  @Override
  protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
    return getSourceInfoStructMaker(SingleStoreConnectorConfig.SOURCE_INFO_STRUCT_MAKER,
        Module.name(), Module.version(), this);
  }

  public SnapshotMode getSnapshotMode() {
    return snapshotMode;
  }

  public Duration getConnectionTimeout() {
    return connectionTimeout;
  }

  public String databaseName() {
    return config.getString(DATABASE_NAME);
  }

  public String hostname() {
    return config.getString(HOSTNAME);
  }

  public int port() {
    return config.getInteger(PORT);
  }

  public String username() {
    return config.getString(USER);
  }

  public String password() {
    return config.getString(PASSWORD);
  }

  public String driverParameters() {
    return config.getString(DRIVER_PARAMETERS);
  }

  public RelationalTableFilters getTableFilters() {
    return tableFilters;
  }

  public Boolean populateInternalId() {
    return populateInternalId;
  }

  public List<String> offsets() {
    return offsets;
  }

  /**
   * Returns the Geography mode Enum configuration. This defaults to {@code geometry} if nothing is
   * provided.
   */
  public GeographyMode getGeographyMode() {
    return GeographyHandlingMode
        .parse(this.getConfig().getString(GEOGRAPHY_HANDLING_MODE))
        .asDecimalMode();
  }

  /**
   * The set of predefined SnapshotMode options or aliases.
   */
  public enum SnapshotMode implements EnumeratedValue {

    /**
     * Perform a snapshot only upon initial startup of a connector.
     */
    INITIAL("initial", true, true, true),

    /**
     * Perform a snapshot and then stop before attempting to stream events.
     */
    INITIAL_ONLY("initial_only", true, true, false),

    /**
     * Perform a snapshot when it is needed.
     */
    WHEN_NEEDED("when_needed", true, true, true),

    /**
     * Perform a snapshot of only the database schemas (without data) and then begin stream events.
     * This should be used with care, but it is very useful when the change event consumers need
     * only the changes from the point in time the snapshot is made (and doesn't care about any
     * state or changes prior to this point).
     */
    NO_DATA("no_data", true, false, true);

    private final String value;
    private final boolean includeSchema;
    private final boolean includeData;
    private final boolean shouldStream;

    SnapshotMode(String value, boolean includeSchema, boolean includeData, boolean shouldStream) {
      this.value = value;
      this.includeSchema = includeSchema;
      this.includeData = includeData;
      this.shouldStream = shouldStream;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value the configuration property value; may not be null
     * @return the matching option, or null if no match is found
     */
    public static SnapshotMode parse(String value) {
      if (value == null) {
        return null;
      }
      value = value.trim();
      for (SnapshotMode option : SnapshotMode.values()) {
        if (option.getValue().equalsIgnoreCase(value)) {
          return option;
        }
      }
      return null;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value        the configuration property value; may not be null
     * @param defaultValue the default value; may be null
     * @return the matching option, or null if no match is found and the non-null default is invalid
     */
    public static SnapshotMode parse(String value, String defaultValue) {
      SnapshotMode mode = parse(value);
      if (mode == null && defaultValue != null) {
        mode = parse(defaultValue);
      }
      return mode;
    }

    @Override
    public String getValue() {
      return value;
    }

    /**
     * Whether this snapshotting mode should include the schema.
     */
    public boolean includeSchema() {
      return includeSchema;
    }

    /**
     * Whether this snapshotting mode should include the actual data or just the schema of captured
     * tables.
     */
    public boolean includeData() {
      return includeData;
    }

    /**
     * Whether the snapshot mode is followed by streaming.
     */
    public boolean shouldStream() {
      return shouldStream;
    }

    /**
     * Whether the snapshot should be executed.
     */
    public boolean shouldSnapshot() {
      return includeSchema || includeData;
    }
  }

  /**
   * The set of predefined SecureConnectionMode options or aliases.
   */
  public enum SecureConnectionMode implements EnumeratedValue {
    /**
     * Establish an unencrypted connection.
     */
    DISABLE("disable"),

    /**
     * Establish a secure (encrypted) connection, but no certificate and hostname validation.
     */
    TRUST("trust"),
    /**
     * Establish a secure (encrypted) connection, but additionally verify the server TLS certificate
     * against the configured Certificate Authority (CA) certificates. The connection attempt fails
     * if no valid matching CA certificates are found.
     */
    VERIFY_CA("verify_ca"),
    /**
     * Like VERIFY_CA, but additionally verify that the server certificate matches the host to which
     * the connection is attempted.
     * <p>
     * see the {@code sslmode} SingleStore JDBC driver option
     */
    VERIFY_FULL("verify-full");

    private final String value;

    SecureConnectionMode(String value) {
      this.value = value;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value the configuration property value; may not be null
     * @return the matching option, or null if no match is found
     */
    public static SecureConnectionMode parse(String value) {
      if (value == null) {
        return null;
      }
      value = value.trim();
      for (SecureConnectionMode option : SecureConnectionMode.values()) {
        if (option.getValue().equalsIgnoreCase(value)) {
          return option;
        }
      }
      return null;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value        the configuration property value; may not be null
     * @param defaultValue the default value; may be null
     * @return the matching option, or null if no match is found and the non-null default is invalid
     */
    public static SecureConnectionMode parse(String value, String defaultValue) {
      SecureConnectionMode mode = parse(value);
      if (mode == null && defaultValue != null) {
        mode = parse(defaultValue);
      }
      return mode;
    }

    @Override
    public String getValue() {
      return value;
    }
  }

  /**
   * The set of predefined GeographyHandlingMode options or aliases.
   */
  public enum GeographyHandlingMode implements EnumeratedValue {
    /**
     * Represent {@code GEOGRAPHY} and {@code GEOGRAPHYPOINT} values as geometry
     * {@link io.debezium.data.geometry.Geometry} values.
     */
    GEOMETRY("geometry"),

    /**
     * Represent {@code GEOGRAPHY} and {@code GEOGRAPHYPOINT} values as a string values.
     */
    STRING("string");

    private final String value;

    GeographyHandlingMode(String value) {
      this.value = value;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value the configuration property value; may not be null
     * @return the matching option, or null if no match is found
     */
    public static GeographyHandlingMode parse(String value) {
      if (value == null) {
        return null;
      }
      value = value.trim();
      for (GeographyHandlingMode option : GeographyHandlingMode.values()) {
        if (option.getValue().equalsIgnoreCase(value)) {
          return option;
        }
      }
      return null;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value        the configuration property value; may not be null
     * @param defaultValue the default value; may be null
     * @return the matching option, or null if no match is found and the non-null default is invalid
     */
    public static GeographyHandlingMode parse(String value, String defaultValue) {
      GeographyHandlingMode mode = parse(value);
      if (mode == null && defaultValue != null) {
        mode = parse(defaultValue);
      }
      return mode;
    }

    @Override
    public String getValue() {
      return value;
    }

    public GeographyMode asDecimalMode() {
      switch (this) {
        case STRING:
          return GeographyMode.STRING;
        case GEOMETRY:
        default:
          return GeographyMode.GEOMETRY;
      }
    }
  }

  private static class SystemTablesPredicate implements TableFilter {

    protected static final List<String> SYSTEM_SCHEMAS = Arrays
        .asList("information_schema", "cluster", "memsql");

    @Override
    public boolean isIncluded(TableId t) {
      return t.catalog() != null && !SYSTEM_SCHEMAS.contains(t.catalog().toLowerCase());
    }
  }
}
