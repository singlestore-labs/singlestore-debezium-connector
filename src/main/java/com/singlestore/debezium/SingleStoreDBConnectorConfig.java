package com.singlestore.debezium;

import io.debezium.config.*;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DefaultTopicNamingStrategy;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigValue;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The configuration properties.
 */
public class SingleStoreDBConnectorConfig extends RelationalDatabaseConnectorConfig {
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 10_240;

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(SingleStoreDBSourceInfoStructMaker.class.getName());

    public Map<String, ConfigValue> validate() {
        return getConfig().validate(ALL_FIELDS);
    }

    protected static final int DEFAULT_PORT = 3306;

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'schema_only': If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures only the schema (table structures), but not any table data. After the snapshot completes, the connector begins to stream changes.; "
                    + "'initial' (default): If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes.; "
                    + "'initial_only': The connector performs a snapshot as it does for the 'initial' option, but after the connector completes the snapshot, it stops, and does not stream changes.");

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
            .withDescription("Additional JDBC parameters to use with connection string to SingleStore server. Format: 'param1=value1; param2 = value2; ...'. The supported parameters are\n" +
                    "available in the `SingleStore Connection String Parameters\n" +
                    "<https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters>`_.");

    public static final Field INCONSISTENT_SCHEMA_HANDLING_MODE = Field.create("inconsistent.schema.handling.mode")
            .withDisplayName("Inconsistent schema failure handling")
            .withEnum(EventProcessingFailureHandlingMode.class, EventProcessingFailureHandlingMode.FAIL)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 2))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription(
                    "Specify how events that belong to a table missing from internal schema representation (i.e. internal representation is not consistent with database) should be handled, including: "
                            + "'fail' (the default) an exception indicating the problematic event is raised, causing the connector to be stopped; "
                            + "'warn' the problematic event will be logged and the event will be skipped; "
                            + "'skip' the problematic event will be skipped.");

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
            .withDescription("The location of the trust store file for the server certificate verification.");

    public static final Field SSL_TRUSTSTORE_PASSWORD = Field.create("database.ssl.truststore.password")
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
        .withDescription("The name of the ефиду from which the connector should capture changes");


    public static final Field TOPIC_NAMING_STRATEGY = CommonConnectorConfig.TOPIC_NAMING_STRATEGY.withDefault(DefaultTopicNamingStrategy.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("SingleStoreDB")
            .excluding(SNAPSHOT_LOCK_TIMEOUT_MS,
                    INCLUDE_SCHEMA_COMMENTS,
                    INCLUDE_SCHEMA_CHANGES,
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    QUERY_FETCH_SIZE,
                    SNAPSHOT_FETCH_SIZE,
                    SNAPSHOT_MAX_THREADS,
                    TABLE_IGNORE_BUILTIN,
                    TABLE_INCLUDE_LIST,
                    TABLE_EXCLUDE_LIST,
                    INCREMENTAL_SNAPSHOT_WATERMARKING_STRATEGY,
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
                    SNAPSHOT_MODE)
            .events(
                    INCONSISTENT_SCHEMA_HANDLING_MODE,
                    SOURCE_INFO_STRUCT_MAKER)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final Configuration config;
    private final SnapshotMode snapshotMode;
    private final EventProcessingFailureHandlingMode inconsistentSchemaFailureHandlingMode;
    private final Duration connectionTimeout;
    private final RelationalTableFilters tableFilters;

    public SingleStoreDBConnectorConfig(Configuration config) {
        super(config,
                new SystemTablesPredicate(),
                t -> t.catalog() + "." + t.table(),
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.CATALOG,
                false);
        this.config = config;
        this.tableFilters = new SingleStoreDBTableFilters(config, new SystemTablesPredicate(), getTableIdMapper(), false);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        final String inconsistentSchemaFailureHandlingMode = config.getString(SingleStoreDBConnectorConfig.INCONSISTENT_SCHEMA_HANDLING_MODE);
        this.inconsistentSchemaFailureHandlingMode = EventProcessingFailureHandlingMode.parse(inconsistentSchemaFailureHandlingMode);
        this.connectionTimeout = Duration.ofMillis(config.getLong(SingleStoreDBConnectorConfig.CONNECTION_TIMEOUT_MS));        
    }

    private static class SystemTablesPredicate implements TableFilter {

        protected static final List<String> SYSTEM_SCHEMAS = Arrays.asList("information_schema", "cluster", "memsql");

        @Override
        public boolean isIncluded(TableId t) {
            return t.catalog() != null && !SYSTEM_SCHEMAS.contains(t.catalog().toLowerCase());
        }
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
        return getSourceInfoStructMaker(SingleStoreDBConnectorConfig.SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public EventProcessingFailureHandlingMode inconsistentSchemaFailureHandlingMode() {
        return inconsistentSchemaFailureHandlingMode;
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
        INITIAL_ONLY("initial_only", true, true, false);
        /**
         * Perform a snapshot of only the database schemas (without data) and then begin stream events.
         * This should be used with care, but it is very useful when the change event consumers need only the changes
         * from the point in time the snapshot is made (and doesn't care about any state or changes prior to this point).
         */
        // TODO: PLAT-6912
        // SCHEMA_ONLY("schema_only", true, false, true);

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
         * Whether this snapshotting mode should include the actual data or just the
         * schema of captured tables.
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
         * Establish a secure (encrypted) connection, but additionally verify the server TLS certificate against the configured Certificate Authority
         * (CA) certificates. The connection attempt fails if no valid matching CA certificates are found.
         */
        VERIFY_CA("verify_ca"),
        /**
         * Like VERIFY_CA, but additionally verify that the server certificate matches the host to which the connection is
         * attempted.
         * <p>
         * see the {@code sslmode} SingleStore JDBC driver option
         */
        VERIFY_FULL("verify-full");

        private final String value;

        SecureConnectionMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
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
    }

}
