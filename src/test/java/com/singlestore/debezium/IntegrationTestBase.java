package com.singlestore.debezium;

import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;

abstract class IntegrationTestBase extends AbstractConnectorTest {

    public static GenericContainer<?> SINGLESTORE_SERVER;
    protected static final String TEST_IMAGE = System.getProperty("singlestoredb.image", "adalbertsinglestore/singlestore-poc-observe");
    protected static Integer TEST_PORT = Integer.parseInt(System.getProperty("singlestoredb.port", "3306"));
    protected static final String TEST_SERVER = System.getProperty("singlestoredb.host", "localhost");
    protected static final String TEST_USER = System.getProperty("singlestoredb.user", "root");
    protected static final String TEST_PASSWORD = System.getProperty("singlestoredb.password", "");
    protected static final String TEST_DATABASE = "db";
    protected static final String TEST_TOPIC_PREFIX = "singlestore_topic";

    @BeforeClass
    public static void init() throws Exception {
        try (SingleStoreDBConnection conn = create()) {
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
     * @return the SingleStoreDBConnection instance; never null
     */
    public static SingleStoreDBConnection create() {
        return new SingleStoreDBConnection(defaultJdbcConnectionConfig());
    }

    /**
     * Executes a JDBC statement using the default jdbc config
     *
     * @param statement A SQL statement
     * @param furtherStatements Further SQL statement(s)
     */
    public static void execute(String statement, String... furtherStatements) {
        if (furtherStatements != null) {
            for (String further : furtherStatements) {
                statement = statement + further;
            }
        }

        try (SingleStoreDBConnection connection = create()) {
            // TODO: JDBC 1.1.9 doesn't support non-auto commit mode.
            // When we will use newer JDBC driver then this can be rewritten to 
            // don't commit changes if at least one query failed.
            connection.execute(statement);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
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
        try (SingleStoreDBConnection connection = create()) {
            connection.readAllTableNames(new String[]{"TABLE"}).forEach(table -> {
                if (table.catalog().equals(TEST_DATABASE)) {
                    execute(String.format("DROP TABLE `%s`.`%s`", table.catalog(), table.table()));
                }
            });
        }
    }

    public static SingleStoreDBConnection.SingleStoreDBConnectionConfiguration defaultJdbcConnectionConfig() {
        return new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(defaultJdbcConfig());
    }

    public static SingleStoreDBConnection.SingleStoreDBConnectionConfiguration defaultJdbcConnectionConfigWithDatabase() {
        return new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(defaultJdbcConfigWithDatabase());
    }

    public static JdbcConfiguration defaultJdbcConfigWithDatabase() {
        return defaultJdbcConfigBuilder()
                .withDefault(SingleStoreDBConnectorConfig.DATABASE_NAME, TEST_DATABASE)
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return defaultJdbcConfigBuilder().build();
    }

    public static JdbcConfiguration.Builder defaultJdbcConfigBuilder() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .with(SingleStoreDBConnectorConfig.TOPIC_PREFIX, TEST_TOPIC_PREFIX)
                .withDefault(SingleStoreDBConnectorConfig.HOSTNAME, TEST_SERVER)
                .withDefault(SingleStoreDBConnectorConfig.PORT, TEST_PORT)
                .withDefault(SingleStoreDBConnectorConfig.USER, TEST_USER)
                .withDefault(SingleStoreDBConnectorConfig.PASSWORD, TEST_PASSWORD)
                .withDefault(SingleStoreDBConnectorConfig.DRIVER_PARAMETERS, "allowMultiQueries=true");
    }

    protected static void executeDDL(String ddlFile) throws Exception {
        URL ddlTestFile = IntegrationTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        String statements = java.nio.file.Files.readAllLines(Paths.get(ddlTestFile.toURI()))
                .stream()
                .collect(Collectors.joining(System.lineSeparator()));
        try (SingleStoreDBConnection connection = create()) {
            connection.execute(statements);
        }
    }

    protected static String topicName(String suffix) {
        return TEST_SERVER + "." + suffix;
    }
}