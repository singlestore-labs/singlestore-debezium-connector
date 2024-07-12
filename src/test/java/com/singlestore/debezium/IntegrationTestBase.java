package com.singlestore.debezium;

import static org.junit.Assert.assertNotNull;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

abstract class IntegrationTestBase extends AbstractConnectorTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestBase.class);
  private static GenericContainer<?> SINGLESTORE_SERVER;
  private static final String TEST_IMAGE = System.getProperty("singlestore.image",
      "ghcr.io/singlestore-labs/singlestoredb-dev:latest");
  static Integer TEST_PORT = Integer.parseInt(System.getProperty("singlestore.port", "3306"));
  static final String TEST_SERVER = System.getProperty("singlestore.hostname", "localhost");
  private static final String TEST_SERVER_VERSION = System.getProperty("singlestore.version", "");
  private static final String TEST_USER = System.getProperty("singlestore.user", "root");
  private static final String TEST_PASSWORD = "password";
  static final String TEST_DATABASE = "db";
  static final String TEST_TOPIC_PREFIX = "singlestore_topic";
  private static final String SINGLESTORE_LICENSE = System.getenv("SINGLESTORE_LICENSE");

  @BeforeClass
  public static void init() throws Exception {
    try (SingleStoreConnection conn = create()) {
      conn.connect();
    } catch (SQLException e) {
      LOGGER.error(e.getSQLState(), e);
      // Failed to connect
      // Assume that docker container is not running and start it
      LOGGER.info("Starting test container: {}, version: {}", TEST_IMAGE, TEST_SERVER_VERSION);
      assert SINGLESTORE_LICENSE != null;
      SINGLESTORE_SERVER = new GenericContainer<>(TEST_IMAGE)
          .waitingFor(Wait.forLogMessage(".*Log Opened.*", 1))
          .withExposedPorts(TEST_PORT)
          .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES))
          .withEnv(Map.of(
              "SINGLESTORE_LICENSE", SINGLESTORE_LICENSE,
              "ROOT_PASSWORD", TEST_PASSWORD,
              "SINGLESTORE_VERSION", TEST_SERVER_VERSION));

      SINGLESTORE_SERVER.start();
      TEST_PORT = SINGLESTORE_SERVER.getFirstMappedPort();
    }

    // Create database if it doesn't exist
    executeDDL("create_database.ddl");
    executeDDL("create_tables.ddl");
    execute("SET GLOBAL enable_observe_queries=1");
  }

  @AfterClass
  public static void deinit() throws Exception {
    if (SINGLESTORE_SERVER != null) {
      SINGLESTORE_SERVER.close();
    }
  }

  @Before
  public void refreshTables() throws Exception {
    deleteAllDataTables();
    clearConsumedEvents();
  }

  /**
   * Obtain a default DB connection.
   *
   * @return the SingleStoreConnection instance; never null
   */
  public static SingleStoreConnection create() {
    return new SingleStoreConnection(defaultJdbcConnectionConfig());
  }

  protected void clearConsumedEvents() {
    consumedLines.clear();
  }

  protected void waitForSnapshotToBeCompleted() throws InterruptedException {
    waitForSnapshotToBeCompleted("singlestore", "singlestore_topic");
  }

  protected void waitForSnapshotWithCustomMetricsToBeCompleted(Map<String, String> props)
      throws InterruptedException {
    waitForSnapshotWithCustomMetricsToBeCompleted("singlestore", "singlestore_topic", props);
  }

  protected void waitForStreamingToStart() throws InterruptedException {
    waitForStreamingRunning("singlestore", "singlestore_topic");
  }

  protected void waitForStreamingWithCustomMetricsToStart(Map<String, String> props)
      throws InterruptedException {
    waitForStreamingWithCustomMetricsToStart("singlestore", "singlestore_topic", props);
  }

  /**
   * Executes a JDBC statement using the default jdbc config
   *
   * @param statement         A SQL statement
   * @param furtherStatements Further SQL statement(s)
   */
  public static void execute(String statement, String... furtherStatements) {
    StringBuilder statementBuilder = new StringBuilder(statement);
    if (furtherStatements != null) {
      for (String further : furtherStatements) {
        statementBuilder.append(further);
      }
    }

    try (SingleStoreConnection connection = create()) {
      connection.setAutoCommit(false);
      connection.executeWithoutCommitting(statementBuilder.toString());
      Connection jdbcConn = connection.connection();
      if (!statement.endsWith("ROLLBACK;")) {
        jdbcConn.commit();
      } else {
        jdbcConn.rollback();
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Drops all tables in TEST_DATABASE.
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

  /**
   * Delete data from all tables in TEST_DATABASE.
   *
   * @throws SQLException if anything fails.
   */
  public static void deleteAllDataTables() throws SQLException {
    try (SingleStoreConnection connection = create()) {
      connection.readAllTableNames(new String[]{"TABLE"}).forEach(table -> {
        if (table.catalog().equals(TEST_DATABASE)) {
          execute(
              String.format("DELETE FROM `%s`.`%s` WHERE 1 = 1", table.catalog(), table.table()));
        }
      });
      connection.execute("SNAPSHOT DATABASE " + TEST_DATABASE + ";");
    }
  }

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
        .withDefault(SingleStoreConnectorConfig.DRIVER_PARAMETERS, "allowMultiQueries=true");
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

  public static class TestAppender extends AppenderBase<ILoggingEvent> {

    private final Stack<ILoggingEvent> events = new Stack<>();

    @Override
    protected void append(ILoggingEvent event) {
      events.add(event);
    }

    public List<ILoggingEvent> getLog() {
      return events;
    }
  }
}
