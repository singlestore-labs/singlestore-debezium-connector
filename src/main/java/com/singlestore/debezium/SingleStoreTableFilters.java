package com.singlestore.debezium;

import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;

public class SingleStoreTableFilters extends RelationalTableFilters {

    private final TableFilter tableFilter;
    private final Predicate<String> databaseFilter;
    private final String tableName;
    private final String databaseName;

    public SingleStoreTableFilters(Configuration config, TableFilter systemTablesFilter,
            TableIdToStringMapper tableIdMapper, boolean useCatalogBeforeSchema) {
        super(config, systemTablesFilter, tableIdMapper, useCatalogBeforeSchema);
        databaseName = config.getString(SingleStoreConnectorConfig.DATABASE_NAME);
        tableName = config.getString(SingleStoreConnectorConfig.TABLE_NAME);

        tableFilter = TableFilter.fromPredicate(
            table -> 
                table.table().equals(tableName) && table.catalog().equals(databaseName)
            );
        databaseFilter = 
            db -> db.equals(databaseName);
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
