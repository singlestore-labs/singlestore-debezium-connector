package com.singlestore.debezium;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.framework;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;

public class IntegrationTestBase {

    public static GenericContainer<?> SINGLESTORE_SERVER;    
    protected static Integer TEST_PORT = Integer.parseInt(System.getProperty("singlestoredb.port", "3306"));
    protected static final String TEST_SERVER = System.getProperty("singlestoredb.host", "localhost");    
    protected static final String TEST_USER = System.getProperty("singlestoredb.user", "root");
    protected static final String TEST_PASSWORD = System.getProperty("singlestoredb.password", "");    
    protected static final String TEST_DATABASE = "db";

    @BeforeClass
    public static void init() throws Exception {
        try (SingleStoreDBConnection conn = create()) {
            conn.connect();
        } catch (SQLException e) {
            // Failed to connect
            // Assume that docker container is not running and start it
            SINGLESTORE_SERVER = new GenericContainer<>("adalbertsinglestore/singlestore-poc-observe")
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
     * Executes a JDBC statement using the default jdbc config without autocommitting the connection
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
            connection.setAutoCommit(false);
            connection.executeWithoutCommitting(statement);
            Connection jdbcConn = connection.connection();
            if (!statement.endsWith("ROLLBACK;")) {
                jdbcConn.commit();
            }
            else {
                jdbcConn.rollback();
            }
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

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .with(SingleStoreDBConnectorConfig.TOPIC_PREFIX, "singlestore-topic")
                .withDefault(SingleStoreDBConnectorConfig.HOSTNAME, TEST_SERVER)
                .withDefault(SingleStoreDBConnectorConfig.PORT, TEST_PORT)
                .withDefault(SingleStoreDBConnectorConfig.USER, TEST_USER)
                .withDefault(SingleStoreDBConnectorConfig.PASSWORD, TEST_PASSWORD)
                .withDefault(SingleStoreDBConnectorConfig.DRIVER_PARAMETERS, "allowMultiQueries=true")
                .build();
    }

    protected static void executeDDL(String ddlFile) throws Exception {
        URL ddlTestFile = IntegrationTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        String statements = Files.readAllLines(Paths.get(ddlTestFile.toURI()))
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
