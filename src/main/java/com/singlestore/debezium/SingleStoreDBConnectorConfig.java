package com.singlestore.debezium;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/**
 * The configuration properties.
 */
public class SingleStoreDBConnectorConfig extends RelationalDatabaseConnectorConfig {
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 10_240;


    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(SingleStoreDBSourceInfoStructMaker.class.getName());

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    // TODO add SingleStoreDB specific fields
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public SingleStoreDBConnectorConfig(Configuration config) {
        super(config,
            new SystemTablesPredicate(),
            // TODO think if we need to escape names
            x -> x.schema() + "." + x.table(),
            DEFAULT_SNAPSHOT_FETCH_SIZE,
            ColumnFilterMode.CATALOG,
            false);
    }

    private static class SystemTablesPredicate implements TableFilter {
        @Override
        public boolean isIncluded(TableId t) {
            // TODO implement
            return true;
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

}
