package com.singlestore.debezium;

import io.debezium.jdbc.JdbcConnection;

/**
 * {@link JdbcConnection} extension to be used with SingleStoreDB
 */
public class SingleStoreDBConnection extends JdbcConnection {

    private static final String QUOTED_CHARACTER = "`";

    public SingleStoreDBConnection(SingleStoreDBConnectorConfig connectionConfig) {
        super(null, null, QUOTED_CHARACTER, QUOTED_CHARACTER);
    }
}
