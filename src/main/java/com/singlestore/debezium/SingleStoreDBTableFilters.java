package com.singlestore.debezium;

import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;

public class SingleStoreDBTableFilters extends RelationalTableFilters {

    private final TableFilter tableFilter;
    private final Predicate<String> databaseFilter;

    public SingleStoreDBTableFilters(Configuration config, TableFilter systemTablesFilter,
            TableIdToStringMapper tableIdMapper, boolean useCatalogBeforeSchema) {
        super(config, systemTablesFilter, tableIdMapper, useCatalogBeforeSchema);
        tableFilter = TableFilter.fromPredicate(
            table -> table.table().equals(config.getString(SingleStoreDBConnectorConfig.TABLE_NAME)));
        databaseFilter = 
            db -> db.equals(config.getString(SingleStoreDBConnectorConfig.DATABASE_NAME));
    }

    @Override
    public TableFilter dataCollectionFilter() {
        return tableFilter;
    }

    @Override
    public TableFilter eligibleForSchemaDataCollectionFilter() {
        return tableFilter;
    }

    @Override
    public Predicate<String> databaseFilter() {
        return databaseFilter;
    }
}
