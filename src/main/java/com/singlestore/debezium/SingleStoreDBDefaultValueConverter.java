package com.singlestore.debezium;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SingleStoreDBDefaultValueConverter implements DefaultValueConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleStoreDBDefaultValueConverter.class);
    private static final String EPOCH_TIMESTAMP = "1970-01-01 00:00:00";
    private static final String EPOCH_DATE = "1970-01-01";
    private static final Pattern TIME_FIELD_PATTERN = Pattern.compile("(\\-?[0-9]*):([0-9]*)(:([0-9]*))?(\\.([0-9]*))?");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart()
            .appendLiteral(" ")
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalEnd()
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .toFormatter();
    private static final DateTimeFormatter DATE_TIME_FORMATTER_WITH_MS = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart()
            .appendLiteral(" ")
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalEnd()
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();
    private static final DateTimeFormatter ISO_LOCAL_DATE_WITH_OPTIONAL_TIME = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral(" ")
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalEnd()
            .toFormatter();

    private final SingleStoreDBValueConverters converters;

    public SingleStoreDBDefaultValueConverter(SingleStoreDBValueConverters converters) {
        this.converters = converters;
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        Object logicalDefaultValue = convert(column, defaultValueExpression);
        if (logicalDefaultValue == null) {
            return Optional.empty();
        }
        final SchemaBuilder schemaBuilder = converters.schemaBuilder(column);
        if (schemaBuilder == null) {
            return Optional.of(logicalDefaultValue);
        }
        final Schema schema = schemaBuilder.build();
        final Field field = new Field(column.name(), -1, schema);
        final ValueConverter valueConverter = converters.converter(column, field);
        Object convertedDefaultValue = valueConverter.convert(logicalDefaultValue);
        if (convertedDefaultValue instanceof Struct) {
            // Workaround for KAFKA-12694
            LOGGER.warn("Struct can't be used as default value for column '{}', will use null instead.", column.name());
            return Optional.empty();
        }
        return Optional.ofNullable(convertedDefaultValue);
    }

    /**
     * Converts a default value from the expected format to a logical object acceptable by the main JDBC
     * converter.
     *
     * @param column column definition
     * @param value  string formatted default value
     * @return value converted to a Java type
     */
    public Object convert(Column column, String value) {
        if (value == null) {
            return value;
        }
        String typeName = column.typeName().toUpperCase();
        switch (typeName) {
            case "DATE":
                return convertToLocalDate(column, value);
            case "TIME":
                return convertToDuration(column, value);
            case "DATETIME":
            case "TIMESTAMP":
                return convertToLocalDateTime(column, value);
            case "FLOAT":
            case "FLOAT UNSIGNED":
                return Float.parseFloat(value);
            case "BIT":
                return value.getBytes(StandardCharsets.UTF_8);
            case "DOUBLE":
            case "DOUBLE UNSIGNED":
                return Double.parseDouble(value);
        }
        return value;
    }

    /**
     * Converts a string object for an object type of {@link LocalDate}, set epoch date 1970-01-01 in case if default
     * value is failed to parse and column is not optional.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value  the string object to be converted into a {@link LocalDate} type
     * @return the converted value;
     */
    private Object convertToLocalDate(Column column, String value) {
        try {
            return LocalDate.from(ISO_LOCAL_DATE_WITH_OPTIONAL_TIME.parse(value));
        } catch (Exception e) {
            LOGGER.warn("Invalid default value '{}' for date column '{}'; {}", value, column.name(), e.getMessage());
            if (column.isOptional()) {
                return null;
            } else {
                return LocalDate.from(ISO_LOCAL_DATE_WITH_OPTIONAL_TIME.parse(EPOCH_DATE));
            }
        }
    }

    /**
     * Converts a string object for an object type of {@link LocalDateTime}, set epoch timestamp 1970-01-01 00:00:00 in case if default
     * * value is failed to parse and column is not optional.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value  the string object to be converted into a {@link LocalDateTime} type;
     * @return the converted value;
     */
    private Object convertToLocalDateTime(Column column, String value) {
        DateTimeFormatter formatter = column.length() > 25 ? DATE_TIME_FORMATTER_WITH_MS : DATE_TIME_FORMATTER;
        try {
            return LocalDateTime.from(formatter.parse(value));
        } catch (Exception e) {
            LOGGER.warn("Invalid default value '{}' for datetime column '{}'; {}", value, column.name(), e.getMessage());
            if (column.isOptional()) {
                return null;
            } else {
                return LocalDateTime.from(formatter.parse(EPOCH_TIMESTAMP));
            }
        }
    }

    /**
     * Converts a string object for an object type of {@link Duration}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value  the string object to be converted into a {@link Duration} type;
     * @return the converted value;
     */
    private Object convertToDuration(Column column, String value) {
        Matcher matcher = TIME_FIELD_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new DebeziumException("Unexpected format for TIME column: " + value);
        }
        final long hours = Long.parseLong(matcher.group(1));
        final long minutes = Long.parseLong(matcher.group(2));
        final String secondsGroup = matcher.group(4);
        long seconds = 0;
        long nanoSeconds = 0;
        if (secondsGroup != null) {
            seconds = Long.parseLong(secondsGroup);
            String microSecondsString = matcher.group(6);
            if (microSecondsString != null) {
                nanoSeconds = Long.parseLong(Strings.justifyLeft(microSecondsString, 9, '0'));
            }
        }
        if (hours >= 0) {
            return Duration.ofHours(hours)
                    .plusMinutes(minutes)
                    .plusSeconds(seconds)
                    .plusNanos(nanoSeconds);
        } else {
            return Duration.ofHours(hours)
                    .minusMinutes(minutes)
                    .minusSeconds(seconds)
                    .minusNanos(nanoSeconds);
        }
    }
}
