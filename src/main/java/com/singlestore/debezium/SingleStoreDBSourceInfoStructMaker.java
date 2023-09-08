package com.singlestore.debezium;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfoStructMaker;

public class SingleStoreDBSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    @Override
    public Schema schema() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'schema'");
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'struct'");
    }
    
}
