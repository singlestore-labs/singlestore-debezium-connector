package com.singlestore.debezium;

import java.util.Optional;

import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;

public class SingleStoreDBDefaultValueConverter implements DefaultValueConverter {

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parseDefaultValue'");
    }
}
