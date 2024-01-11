package com.singlestore.debezium;

import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;

public class SingleStoreDBTableFilters extends RelationalTableFilters {

    private final TableFilter tableFilter;
    private final Predicate<String> databaseFilter;
    private final String tableName;
    private final String databaseName;
    private final TableIdToStringMapper mapper;

    public SingleStoreDBTableFilters(Configuration config, TableFilter systemTablesFilter,
            TableIdToStringMapper tableIdMapper, boolean useCatalogBeforeSchema) {
        super(config, systemTablesFilter, tableIdMapper, useCatalogBeforeSchema);
        
        mapper = tableIdMapper;
        databaseName = config.getString(SingleStoreDBConnectorConfig.DATABASE_NAME);
        tableName = tableIdMapper.toString(new TableId(databaseName, null, 
            config.getString(SingleStoreDBConnectorConfig.TABLE_NAME)));

        tableFilter = TableFilter.fromPredicate(
            table -> 
                mapper.toString(table).equals(tableName)
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
