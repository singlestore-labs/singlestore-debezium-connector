package com.singlestore.debezium;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

import java.util.ArrayList;

public class SingleStoreSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("com.singlestore.debezium.Source")
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.PARTITIONID_KEY, Schema.INT32_SCHEMA)
                .field(SourceInfo.OFFSETS_KEY, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
                assert sourceInfo.database() != null
                && sourceInfo.table() != null
                && sourceInfo.txId() != null
                && sourceInfo.partitionId() != null;

        Struct result = super.commonStruct(sourceInfo);
        result.put(SourceInfo.TABLE_NAME_KEY, sourceInfo.table());
        result.put(SourceInfo.TXID_KEY, sourceInfo.txId());
        result.put(SourceInfo.PARTITIONID_KEY, sourceInfo.partitionId());
        result.put(SourceInfo.OFFSETS_KEY, new ArrayList(sourceInfo.offsets()));
        
        return result;
    }    
}
