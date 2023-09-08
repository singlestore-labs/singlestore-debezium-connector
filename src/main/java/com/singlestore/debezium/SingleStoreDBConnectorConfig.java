package com.singlestore.debezium;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;

public class SingleStoreDBConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {
    
    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(SingleStoreDBSourceInfoStructMaker.class.getName());

    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public SingleStoreDBConnectorConfig(Configuration config) {
        super(SingleStoreDBConnector.class,
            config,
            new SystemTablesPredicate(),
            // TODO: think if we need to escape names
            x -> x.schema() + "." + x.table(),
            true,
            DEFAULT_SNAPSHOT_FETCH_SIZE,
            ColumnFilterMode.CATALOG,
            false);
    }

    private static class SystemTablesPredicate implements TableFilter {
        @Override
        public boolean isIncluded(TableId t) {
            // TODO: implement
            return true;
        }
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                // TODO: implement
                return true;
            }
        };
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
