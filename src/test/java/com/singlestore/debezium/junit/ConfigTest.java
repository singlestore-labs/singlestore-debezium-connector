package com.singlestore.debezium.junit;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.singlestore.debezium.SingleStoreDBConnectorConfig;

public class ConfigTest {

    @Test
    public void test() {
        System.out.println("AAA");
        SingleStoreDBConnectorConfig.ALL_FIELDS.forEach(
            field -> {
                System.out.println(field.name() + " - " + field.description());
            }
        );
    }
}