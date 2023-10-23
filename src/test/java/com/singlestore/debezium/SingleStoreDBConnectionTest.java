package com.singlestore.debezium;

import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnId;
import io.debezium.relational.TableId;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class SingleStoreDBConnectionTest {

    @Test
    public void testJdbcParameters() {
        SingleStoreDBConnection connection = createConnectionWithParams(Map.of(SingleStoreDBConnectorConfig.DRIVER_PARAMETERS, "param1=value1;param2=value2;param3=value3"));
        assertEquals(Map.of("param1", "value1", "param2", "value2", "param3", "value3"), connection.connectionConfig().driverParameters());
    }

    @Test
    public void testSslDisabledParams() {
        SingleStoreDBConnection connection = createConnectionWithParams(
                Map.of(SingleStoreDBConnectorConfig.SSL_TRUSTSTORE, "trustStorePath", SingleStoreDBConnectorConfig.SSL_TRUSTSTORE_PASSWORD, "pass",
                        SingleStoreDBConnectorConfig.SSL_KEYSTORE, "keyStorePath", SingleStoreDBConnectorConfig.SSL_KEYSTORE_PASSWORD, "pass"));
        assertNull(connection.connectionConfig().config().getString("trustStorePassword"));
        assertNull(connection.connectionConfig().config().getString("keyStorePassword"));
        assertNull(connection.connectionConfig().config().getString("keyStore"));
        assertNull(connection.connectionConfig().config().getString("trustStore"));
    }

    @Test
    public void testSslVerifyParams() {
        SingleStoreDBConnection connection = createConnectionWithParams(
                Map.of(SingleStoreDBConnectorConfig.SSL_MODE, "verify_ca", SingleStoreDBConnectorConfig.SSL_TRUSTSTORE, "trustStorePath", SingleStoreDBConnectorConfig.SSL_TRUSTSTORE_PASSWORD, "pass",
                        SingleStoreDBConnectorConfig.SSL_KEYSTORE, "keyStorePath", SingleStoreDBConnectorConfig.SSL_KEYSTORE_PASSWORD, "pass"));
        assertEquals("pass", connection.connectionConfig().config().getString("trustStorePassword"));
        assertEquals("pass", connection.connectionConfig().config().getString("keyStorePassword"));
        assertEquals("file:keyStorePath", connection.connectionConfig().config().getString("keyStore"));
        assertEquals("file:trustStorePath", connection.connectionConfig().config().getString("trustStore"));
    }

    @Test
    public void testQueryFetchSizeParam() {
        SingleStoreDBConnection connection = createConnectionWithParams(Collections.emptyMap());
        assertEquals("1", connection.connectionConfig().config().getString("defaultFetchSize"));
    }

    @Test
    public void testObserveNoParams() throws SQLException {
        SingleStoreDBConnection connection = spy(createConnectionWithParams(Collections.emptyMap()));
        doReturn(connection).when(connection).query(anyString(), any());
        connection.observe(Collections.emptySet(), rs -> {
        });
        verify(connection).query(eq("OBSERVE * FROM *"), any());
    }

    @Test
    public void testObserveWithTableAndColumnFilter() throws SQLException {
        SingleStoreDBConnection connection = spy(createConnectionWithParams(Collections.emptyMap()));
        doReturn(connection).when(connection).query(anyString(), any());
        connection.observe(
                Set.of(ColumnId.parse("debezium.table1.field1"), ColumnId.parse("debezium.table2.field1")),
                Set.of(TableId.parse("debezium.table1"), TableId.parse("debezium.table2")), rs -> {
                });
        verify(connection).query(matches("OBSERVE `debezium`.`table[12]`.`field1`,`debezium`.`table[21]`.`field1` FROM `debezium`.`table[12]`,`debezium`.`table[12]`"), any());
    }

    @Test
    public void testObserveWithFilter() throws SQLException {
        SingleStoreDBConnection connection = spy(createConnectionWithParams(Collections.emptyMap()));
        doReturn(connection).when(connection).query(anyString(), any());
        connection.observe(
                Set.of(ColumnId.parse("debezium.table1.field1"), ColumnId.parse("debezium.table2.field1")),
                Set.of(TableId.parse("debezium.table1"), TableId.parse("debezium.table2")),
                Optional.of(SingleStoreDBConnection.OBSERVE_OUTPUT_FORMAT.JSON),
                Optional.empty(),
                Optional.of("(1, 2, NULL, 4)"),
                Optional.of("`table1`.`filed1`=1"),
                rs -> {
                });
        verify(connection).query(matches("OBSERVE `debezium`.`table[12]`.`field1`,`debezium`.`table[21]`.`field1` FROM `debezium`.`table[12]`,`debezium`.`table[12]` AS JSON BEGIN AT \\(1, 2, NULL, 4\\) WHERE `table1`.`filed1`=1"), any());
    }

    private SingleStoreDBConnection createConnectionWithParams(Map<Field, String> fieldMap) {
        JdbcConfiguration.Builder builder = JdbcConfiguration.create()
                .withDefault(SingleStoreDBConnectorConfig.HOSTNAME, "localhost")
                .withDefault(SingleStoreDBConnectorConfig.PORT, 3306)
                .withDefault(SingleStoreDBConnectorConfig.USER, "root")
                .withDefault(SingleStoreDBConnectorConfig.PASSWORD, "");
        fieldMap.forEach(builder::with);
        return new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(builder.build()));
    }
}
