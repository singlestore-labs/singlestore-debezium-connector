package com.singlestore.debezium;

import java.util.Properties;

import io.debezium.util.IoUtil;

/*
 * Information about this module.
 */
public class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class,
            "com/singlestore/debezium/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    public static String debeziumVersion() {
        return INFO.getProperty("debezium.version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "singlestore";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "SingleStore";
    }

}
