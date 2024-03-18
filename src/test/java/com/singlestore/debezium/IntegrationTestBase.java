package com.singlestore.debezium;

import static org.junit.Assert.assertNotNull;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;

abstract class IntegrationTestBase extends AbstractConnectorTest {

    public static GenericContainer<?> SINGLESTORE_SERVER;
<<<<<<< Updated upstream
    protected static final String TEST_IMAGE = System.getProperty("singlestoredb.image", "adalbertsinglestore/singlestore-poc-observe");
    protected static Integer TEST_PORT = Integer.parseInt(System.getProperty("singlestoredb.port", "3306"));
    protected static final String TEST_SERVER = System.getProperty("singlestoredb.host", "localhost");
    protected static final String TEST_USER = System.getProperty("singlestoredb.user", "root");
    protected static final String TEST_PASSWORD = System.getProperty("singlestoredb.password", "");
=======
    protected static final String TEST_IMAGE = System.getProperty("singlestore.image",
            "adalbertsinglestore/singlestore-poc-observe-v2");
    protected static Integer TEST_PORT = Integer.parseInt(System.getProperty("singlestore.port",
            "3306"));
    protected static final String TEST_SERVER = System.getProperty("singlestore.hostname",
            "localhost");
    protected static final String TEST_USER = System.getProperty("singlestore.user", "root");
    protected static final String TEST_PASSWORD = System.getProperty("singlestore.password", "1");
>>>>>>> Stashed changes
    protected static final String TEST_DATABASE = "db";
    protected static final String TEST_TOPIC_PREFIX = "singlestore_topic";

    @BeforeClass
    public static void init() throws Exception {
        try (SingleStoreConnection conn = create()) {
            conn.connect();
        } catch (SQLException e) {
            // Failed to connect
            // Assume that docker container is not running and start it
            SINGLESTORE_SERVER = new GenericContainer<>(TEST_IMAGE)
                    .withExposedPorts(3306);
            SINGLESTORE_SERVER.start();
            TEST_PORT = SINGLESTORE_SERVER.getFirstMappedPort();
        }

        // Create database if it doesn't exist
        executeDDL("create_database.ddl");

        // Refresh tables
        dropAllTables();
        executeDDL("create_tables.ddl");
    }

    @AfterClass
    public static void deinit() throws Exception {
        if (SINGLESTORE_SERVER != null) {
            SINGLESTORE_SERVER.close();
        }
    }

    /**
     * Obtain a default DB connection.
     *
     * @return the SingleStoreConnection instance; never null
     */
    public static SingleStoreConnection create() {
        return new SingleStoreConnection(defaultJdbcConnectionConfig());
    }

    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("singlestore", "singlestore_topic");
    }

    protected void waitForSnapshotWithCustomMetricsToBeCompleted(
            Map<String, String> props) throws InterruptedException {
        waitForSnapshotWithCustomMetricsToBeCompleted("singlestore", "singlestore_topic", props);
    }

    protected void waitForStreamingToStart() throws InterruptedException {
        waitForStreamingRunning("singlestore", "singlestore_topic");
    }

    protected void waitForStreamingWithCustomMetricsToStart(
            Map<String, String> props) throws InterruptedException {
        waitForStreamingWithCustomMetricsToStart("singlestore", "singlestore_topic", props);
    }

    /**
     * Executes a JDBC statement using the default jdbc config
     *
     * @param statement         A SQL statement
     * @param furtherStatements Further SQL statement(s)
     */
    public static void execute(String statement, String... furtherStatements) {
        if (furtherStatements != null) {
            for (String further : furtherStatements) {
                statement = statement + further;
            }
        }

        try (SingleStoreConnection connection = create()) {
            // TODO: JDBC 1.1.9 doesn't support non-auto commit mode.
            // When we will use newer JDBC driver then this can be rewritten to 
            // don't commit changes if at least one query failed.
            connection.execute(statement);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Drops all tables in TEST_DATABASE.
     *
     *
     * @throws SQLException if anything fails.
     */
    public static void dropAllTables() throws SQLException {
        try (SingleStoreConnection connection = create()) {
            connection.readAllTableNames(new String[]{"TABLE"}).forEach(table -> {
                if (table.catalog().equals(TEST_DATABASE)) {
                    execute(String.format("DROP TABLE `%s`.`%s`", table.catalog(), table.table()));
                }
            });
        }
    }

<<<<<<< Updated upstream
=======
    /**
     *
     * Delete data from all tables in TEST_DATABASE.
     *
     *
     * @throws SQLException if anything fails.
     */
    public static void deleteAllDataTables() throws SQLException {
        try (SingleStoreConnection connection = create()) {
            connection.execute("DELETE FROM db.product WHERE id >= 0");
            connection.execute("SNAPSHOT DATABASE " + TEST_DATABASE + ";");
        }
    }

>>>>>>> Stashed changes
    public static SingleStoreConnection.SingleStoreConnectionConfiguration defaultJdbcConnectionConfig() {
        return new SingleStoreConnection.SingleStoreConnectionConfiguration(defaultJdbcConfig());
    }

    public static SingleStoreConnection.SingleStoreConnectionConfiguration defaultJdbcConnectionConfigWithTable(
            String table) {
        return new SingleStoreConnection.SingleStoreConnectionConfiguration(
                defaultJdbcConfigWithTable(table));
    }

    public static JdbcConfiguration defaultJdbcConfigWithTable(String table) {
        return defaultJdbcConfigBuilder()
                .withDefault(SingleStoreConnectorConfig.DATABASE_NAME, TEST_DATABASE)
                .withDefault(SingleStoreConnectorConfig.TABLE_NAME, table)
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return defaultJdbcConfigBuilder().build();
    }

    public static JdbcConfiguration.Builder defaultJdbcConfigBuilder() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .with(SingleStoreConnectorConfig.TOPIC_PREFIX, TEST_TOPIC_PREFIX)
                .withDefault(SingleStoreConnectorConfig.HOSTNAME, TEST_SERVER)
                .withDefault(SingleStoreConnectorConfig.PORT, TEST_PORT)
                .withDefault(SingleStoreConnectorConfig.USER, TEST_USER)
                .withDefault(SingleStoreConnectorConfig.PASSWORD, TEST_PASSWORD)
                .withDefault(SingleStoreConnectorConfig.DRIVER_PARAMETERS,
                        "allowMultiQueries=true");
    }

    protected static void executeDDL(String ddlFile) throws Exception {
        URL ddlTestFile = IntegrationTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        String statements = java.nio.file.Files.readAllLines(Paths.get(ddlTestFile.toURI()))
                .stream()
                .collect(Collectors.joining(System.lineSeparator()));
        try (SingleStoreConnection connection = create()) {
            connection.execute(statements);
        }
    }

    protected static String topicName(String suffix) {
        return TEST_SERVER + "." + suffix;
    }
}
