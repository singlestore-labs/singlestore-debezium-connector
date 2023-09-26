package com.singlestore.debezium;

import com.singlestore.jdbc.Connection;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class SingleStoreDBConnectionIT {

    @ClassRule
    public static GenericContainer SINGLESTORE_SERVER
            = new GenericContainer("singlestore-poc-observe-v1")
            .withExposedPorts(3306);

    static {
        SINGLESTORE_SERVER.start();
    }

    private static final Integer EXPOSED_PORT = SINGLESTORE_SERVER.getFirstMappedPort();
    private static final String DATABASE = "debezium";

    @BeforeClass
    public static void init() throws SQLException {
        try (Connection connection = (Connection) DriverManager.getConnection("jdbc:singlestore://localhost:" + EXPOSED_PORT + "/?user=root");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + DATABASE);
            statement.execute("USE " + DATABASE);
            statement.execute("DROP TABLE IF EXISTS person");
            statement.execute("DROP TABLE IF EXISTS product");
            statement.execute("DROP TABLE IF EXISTS purchased");
            statement.execute("CREATE TABLE person ("
                    + "  name VARCHAR(255) primary key,"
                    + "  birthdate DATE NULL,"
                    + "  age INTEGER NULL DEFAULT 10,"
                    + "  salary DECIMAL(5,2),"
                    + "  bitStr BIT(18),"
                    + "  sort key(name),"
                    + "  unique key(age)"
                    + ")");
            statement.execute("CREATE TABLE product ("
                    + "  id INT NOT NULL AUTO_INCREMENT,"
                    + "  createdByDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                    + "  modifiedDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "  PRIMARY KEY(id)"
                    + ")");
            statement.execute("CREATE TABLE purchased ("
                    + "  purchaser VARCHAR(255) NOT NULL,"
                    + "  productId INT NOT NULL,"
                    + "  purchaseDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                    + "  PRIMARY KEY(productId,purchaser)"
                    + ")");
        }
    }

    @Test
    public void testConnection() {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
            conn.connect();
            assertTrue(conn.isConnected());
            assertTrue(conn.isValid());
            assertEquals("jdbc:singlestore://localhost:" + EXPOSED_PORT + "/?connectTimeout=30000", conn.connectionString());
            conn.close();
            assertFalse(conn.isConnected());
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPrepareQuery() {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
            conn.execute("use " + DATABASE);
            conn.prepareQuery("insert into person values(?, ?, ?, ?, ?)", ps -> {
                ps.setString(1, "product4");
                ps.setDate(2, Date.valueOf(LocalDate.now()));
                ps.setInt(3, 5);
                ps.setInt(4, 100);
                ps.setBoolean(5, true);
            }, null);
            conn.query("select * from person where name = 'product4'", rs -> {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "product4");
                assertEquals(rs.getInt(3), 5);
                assertEquals(rs.getInt(4), 100);
                assertTrue(rs.getBoolean(5));
            });
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetCurrentTimeStamp() {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
            Optional<Instant> timeStamp = conn.getCurrentTimestamp();
            assertTrue(timeStamp.isPresent());
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMetadata() {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
            Set<TableId> tableIds = conn.readAllTableNames(new String[]{"TABLE", "VIEW"}).stream().filter(t -> t.catalog().equals(DATABASE)).collect(Collectors.toSet());
            Set<String> tableNames = tableIds.stream().map(TableId::table).collect(Collectors.toSet());
            assertEquals("readAllTableNames returns a wrong number of tables", 3, tableIds.size());
            assertTrue("readAllTableNames doesn't contain correct table names", tableNames.containsAll(Arrays.asList("person", "product", "purchased")));
            Set<String> catalogNames = conn.readAllCatalogNames();
            assertTrue("readAllCatalogNames returns a wrong catalog name", catalogNames.contains("debezium"));
            tableNames = conn.readTableNames("debezium", "", "person", new String[]{"TABLE", "VIEW"})
                    .stream().map(TableId::table).collect(Collectors.toSet());
            assertTrue("readTableNames returns a wrong table name", tableNames.contains("person"));
            TableId person = tableIds.stream().filter(t -> t.table().equals("person")).findFirst().orElseThrow();
            List<String> pkList = conn.readPrimaryKeyNames(conn.connection().getMetaData(), person);
            assertTrue(pkList.contains("name"));
            List<String> uniqueList = conn.readTableUniqueIndices(conn.connection().getMetaData(), person);
            assertTrue(uniqueList.contains("age"));
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testReadSchemaMetadata() {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
            Tables tables = new Tables();
            conn.readSchema(tables, DATABASE, null, null, null, true);
            assertThat(tables.size()).isEqualTo(3);
            Table person = tables.forTable(DATABASE, null, "person");
            assertThat(person).isNotNull();
            assertThat(person.filterColumns(col -> col.isAutoIncremented())).isEmpty();
            assertThat(person.primaryKeyColumnNames()).containsOnly("name");
            assertThat(person.retrieveColumnNames()).containsExactly("name", "birthdate", "age", "salary", "bitStr");
            assertThat(person.columnWithName("name").name()).isEqualTo("name");
            assertThat(person.columnWithName("name").typeName()).isEqualTo("VARCHAR");
            assertThat(person.columnWithName("name").jdbcType()).isEqualTo(Types.VARCHAR);
            assertThat(person.columnWithName("name").length()).isEqualTo(255);
            assertFalse(person.columnWithName("name").scale().isPresent());
            assertThat(person.columnWithName("name").position()).isEqualTo(1);
            assertThat(person.columnWithName("name").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("name").isGenerated()).isFalse();
            assertThat(person.columnWithName("name").isOptional()).isFalse();
            assertThat(person.columnWithName("birthdate").name()).isEqualTo("birthdate");
            assertThat(person.columnWithName("birthdate").typeName()).isEqualTo("DATE");
            assertThat(person.columnWithName("birthdate").jdbcType()).isEqualTo(Types.DATE);
            assertThat(person.columnWithName("birthdate").length()).isEqualTo(10);
            assertFalse(person.columnWithName("birthdate").scale().isPresent());
            assertThat(person.columnWithName("birthdate").position()).isEqualTo(2);
            assertThat(person.columnWithName("birthdate").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("birthdate").isGenerated()).isFalse();
            assertThat(person.columnWithName("birthdate").isOptional()).isTrue();
            assertThat(person.columnWithName("age").name()).isEqualTo("age");
            assertThat(person.columnWithName("age").typeName()).isEqualTo("INT");
            assertThat(person.columnWithName("age").jdbcType()).isEqualTo(Types.INTEGER);
            assertThat(person.columnWithName("age").length()).isEqualTo(10);
            assertThat(!person.columnWithName("age").scale().isPresent());
            assertThat(person.columnWithName("age").position()).isEqualTo(3);
            assertThat(person.columnWithName("age").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("age").isGenerated()).isFalse();
            assertThat(person.columnWithName("age").isOptional()).isTrue();
            assertThat(person.columnWithName("salary").name()).isEqualTo("salary");
            assertThat(person.columnWithName("salary").typeName()).isEqualTo("DECIMAL");
            assertThat(person.columnWithName("salary").jdbcType()).isEqualTo(Types.DECIMAL);
            assertThat(person.columnWithName("salary").length()).isEqualTo(5);
            assertThat(person.columnWithName("salary").scale().get()).isEqualTo(2);
            assertThat(person.columnWithName("salary").position()).isEqualTo(4);
            assertThat(person.columnWithName("salary").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("salary").isGenerated()).isFalse();
            assertThat(person.columnWithName("salary").isOptional()).isTrue();
            assertThat(person.columnWithName("bitStr").name()).isEqualTo("bitStr");
            assertThat(person.columnWithName("bitStr").typeName()).isEqualTo("BIT");
            assertThat(person.columnWithName("bitStr").jdbcType()).isEqualTo(Types.BIT);
            assertThat(person.columnWithName("bitStr").length()).isEqualTo(18);
            assertFalse(person.columnWithName("bitStr").scale().isPresent());
            assertThat(person.columnWithName("bitStr").position()).isEqualTo(5);
            assertThat(person.columnWithName("bitStr").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("bitStr").isGenerated()).isFalse();
            assertThat(person.columnWithName("bitStr").isOptional()).isTrue();
            tables = new Tables();
            conn.readSchema(tables, DATABASE, null, null, null, true);
            Table product = tables.forTable(DATABASE, null, "product");
            assertThat(product).isNotNull();
            List<Column> autoIncColumns = product.filterColumns(Column::isAutoIncremented);
            assertThat(autoIncColumns.get(0).name()).isEqualTo("id");
            assertThat(product.primaryKeyColumnNames()).containsOnly("id");
            assertThat(product.retrieveColumnNames()).containsExactly("id", "createdByDate", "modifiedDate");
            assertThat(product.columnWithName("id").name()).isEqualTo("id");
            assertThat(product.columnWithName("id").typeName()).isEqualTo("INT");
            assertThat(product.columnWithName("id").jdbcType()).isEqualTo(Types.INTEGER);
            assertThat(product.columnWithName("id").length()).isEqualTo(10);
            assertThat(!product.columnWithName("id").scale().isPresent()
                    || product.columnWithName("id").scale().get() == 0);
            assertThat(product.columnWithName("id").position()).isEqualTo(1);
            assertThat(product.columnWithName("id").isAutoIncremented()).isTrue();
            assertThat(product.columnWithName("id").isGenerated()).isFalse();
            assertThat(product.columnWithName("id").isOptional()).isFalse();
            assertThat(product.columnWithName("createdByDate").name()).isEqualTo("createdByDate");
            assertThat(product.columnWithName("createdByDate").typeName()).isEqualTo("DATETIME");
            assertThat(product.columnWithName("createdByDate").jdbcType()).isEqualTo(Types.TIMESTAMP);
            assertThat(product.columnWithName("createdByDate").length()).isEqualTo(19);
            assertFalse(product.columnWithName("createdByDate").scale().isPresent());
            assertThat(product.columnWithName("createdByDate").position()).isEqualTo(2);
            assertThat(product.columnWithName("createdByDate").isAutoIncremented()).isFalse();
            assertThat(product.columnWithName("createdByDate").isOptional()).isFalse();
            assertThat(product.columnWithName("modifiedDate").name()).isEqualTo("modifiedDate");
            assertThat(product.columnWithName("modifiedDate").typeName()).isEqualTo("DATETIME");
            assertThat(product.columnWithName("modifiedDate").jdbcType()).isEqualTo(Types.TIMESTAMP);
            assertThat(product.columnWithName("modifiedDate").length()).isEqualTo(19);
            assertFalse(product.columnWithName("modifiedDate").scale().isPresent());
            assertThat(product.columnWithName("modifiedDate").position()).isEqualTo(3);
            assertThat(product.columnWithName("modifiedDate").isAutoIncremented()).isFalse();
            assertThat(product.columnWithName("modifiedDate").isOptional()).isFalse();
            tables = new Tables();
            conn.readSchema(tables, DATABASE, null, null, null, true);
            Table purchased = tables.forTable(DATABASE, null, "purchased");
            assertThat(purchased).isNotNull();
            assertThat(person.filterColumns(col -> col.isAutoIncremented())).isEmpty();
            assertThat(purchased.primaryKeyColumnNames()).containsOnly("productId", "purchaser");
            assertThat(purchased.retrieveColumnNames()).containsExactly("purchaser", "productId", "purchaseDate");
            assertThat(purchased.columnWithName("purchaser").name()).isEqualTo("purchaser");
            assertThat(purchased.columnWithName("purchaser").typeName()).isEqualTo("VARCHAR");
            assertThat(purchased.columnWithName("purchaser").jdbcType()).isEqualTo(Types.VARCHAR);
            assertThat(purchased.columnWithName("purchaser").length()).isEqualTo(255);
            assertFalse(purchased.columnWithName("purchaser").scale().isPresent());
            assertThat(purchased.columnWithName("purchaser").position()).isEqualTo(1);
            assertThat(purchased.columnWithName("purchaser").isAutoIncremented()).isFalse();
            assertThat(purchased.columnWithName("purchaser").isGenerated()).isFalse();
            assertThat(purchased.columnWithName("purchaser").isOptional()).isFalse();
            assertThat(purchased.columnWithName("productId").name()).isEqualTo("productId");
            assertThat(purchased.columnWithName("productId").typeName()).isEqualTo("INT");
            assertThat(purchased.columnWithName("productId").jdbcType()).isEqualTo(Types.INTEGER);
            assertThat(purchased.columnWithName("productId").length()).isEqualTo(10);
            assertThat(!purchased.columnWithName("productId").scale().isPresent());
            assertThat(purchased.columnWithName("productId").position()).isEqualTo(2);
            assertThat(purchased.columnWithName("productId").isAutoIncremented()).isFalse();
            assertThat(purchased.columnWithName("productId").isGenerated()).isFalse();
            assertThat(purchased.columnWithName("productId").isOptional()).isFalse();
            assertThat(purchased.columnWithName("purchaseDate").name()).isEqualTo("purchaseDate");
            assertThat(purchased.columnWithName("purchaseDate").typeName()).isEqualTo("DATETIME");
            assertThat(purchased.columnWithName("purchaseDate").jdbcType()).isEqualTo(Types.TIMESTAMP);
            assertThat(purchased.columnWithName("purchaseDate").length()).isEqualTo(19);
            assertFalse(purchased.columnWithName("purchaseDate").scale().isPresent());
            assertThat(purchased.columnWithName("purchaseDate").position()).isEqualTo(3);
            assertThat(purchased.columnWithName("purchaseDate").isAutoIncremented()).isFalse();
            assertThat(purchased.columnWithName("purchaseDate").isOptional()).isFalse();
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testObserve() {
        try (SingleStoreDBConnection conn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
            String tempTableName = "person_temporary";
            conn.execute("use " + DATABASE,
                    "DROP TABLE IF EXISTS " + tempTableName,
                    "CREATE TABLE " + tempTableName + " ("
                            + "  name VARCHAR(255) primary key,"
                            + "  birthdate DATE NULL,"
                            + "  age INTEGER NULL DEFAULT 10,"
                            + "  salary DECIMAL(5,2),"
                            + "  bitStr BIT(18)"
                            + ")",
                    "insert into " + tempTableName + " values('product1', NOW(), 1, 300, 1)",
                    "insert into " + tempTableName + " values('product2', NOW(), 2, 400, 0)",
                    "delete from " + tempTableName + " where name = 'product1'");
            String[] expectedTypesOrder = {"Insert", "CommitTransaction", "BeginTransaction", "Insert", "CommitTransaction", "BeginTransaction", "Delete", "CommitTransaction"};
            List<String> actualTypes = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            Thread observer = new Thread(() -> {
                try (SingleStoreDBConnection observerConn = new SingleStoreDBConnection(defaultJdbcConnectionConfig())) {
                    Set<TableId> tableIds = observerConn.readAllTableNames(new String[]{"TABLE", "VIEW"}).stream().filter(t -> t.catalog().equals(DATABASE)).collect(Collectors.toSet());
                    Set<TableId> person = tableIds.stream().filter(t -> t.table().equals(tempTableName)).collect(Collectors.toSet());
                    observerConn.observe(person, rs -> {
                        int counter = 0;
                        while (counter < expectedTypesOrder.length && rs.next()) {
                            actualTypes.add(rs.getString(3));
                            counter++;
                        }
                        latch.countDown();
                    });
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
            observer.start();
            latch.await();
            observer.interrupt();
            for (int i = 0; i < expectedTypesOrder.length; i++) {
                assertEquals(expectedTypesOrder[i], actualTypes.get(i));
            }
            conn.execute("DROP TABLE " + tempTableName);
        } catch (SQLException | InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }

    public static SingleStoreDBConnection.SingleStoreDBConnectionConfiguration defaultJdbcConnectionConfig() {
        return new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(defaultJdbcConfig());
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(SingleStoreDBConnectorConfig.HOSTNAME, "localhost")
                .withDefault(SingleStoreDBConnectorConfig.PORT, EXPOSED_PORT)
                .withDefault(SingleStoreDBConnectorConfig.USER, "root")
                .withDefault(SingleStoreDBConnectorConfig.PASSWORD, "")
                .build();
    }
}
