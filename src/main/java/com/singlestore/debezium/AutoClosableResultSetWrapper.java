package com.singlestore.debezium;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AutoClosableResultSetWrapper implements AutoCloseable {

    private final ResultSet resultSet;

    private AutoClosableResultSetWrapper(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public static AutoClosableResultSetWrapper from(ResultSet resultSet) {
        return new AutoClosableResultSetWrapper(resultSet);
    }

    @Override
    public void close() {
        try {
            if (!isClosed() && !isAfterLast()) { // Result set is still open and not exhausted
                getStatement().cancel();
                ((com.singlestore.jdbc.Connection) getStatement().getConnection()).cancelCurrentQuery();
                resultSet.close();
            }
        } catch (Exception ignored) {
        }
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    private Statement getStatement() throws SQLException {
        return resultSet.getStatement();
    }

    private boolean isAfterLast() throws SQLException {
        return resultSet.isAfterLast();
    }

    private boolean isClosed() throws SQLException {
        return resultSet.isClosed();
    }
}
