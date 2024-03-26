# SingleStore connector for Debezium
<!--TODO add latest connector version-->
[![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt) 
[![Github Actions status image](https://github.com/singlestore-labs/singlestore-debezium-connector/actions/workflows/maven.yml/badge.svg)](https://github.com/singlestore-labs/singlestore-debezium-connector/actions)

**Note**: This connector is in private preview. It is intended for experimental use only and is not fully tested, documented, or supported by SingleStore.

SingleStore connector for Debezium ("the connector") captures and records row-level changes that occur in the database. 
You can configure the connector to read from a single table and to ignore, mask, or truncate values in specific columns.

## TOC
 - [Getting started](#getting-started)
 - [How the SingleStore Debezium connector works](#how-the-singlestore-debezium-connector-works)
 - [Data change events](#data-change-events)
 - [Data type mappings](#data-type-mappings)
 - [Connector properties](#connector-properties)
<!--TODO add compatibility information-->
## Getting started

### Prerequisites
Ensure that the following are installed:
 * ZooKeeper
 * Kafka
 * Kafka Connect

### Installation
 * Download the SingleStore connector for Debezium plugin archive <!--TODO add link-->.
 * Extract the archive to a directory.
 * Add the directory from the previous step to Kafka Connect’s plugin path.
 Set the `plugin.path` property in the `connect-distributed.properties` file.
 * Restart the Kafka Connect process to pick up the new JARs.

### Usage
After the connector is installed, you can register it using `curl` command.

```
curl -i -X POST \
 -H "Accept:application/json" \
 -H "Content-Type:application/json" \
 <Kafka Connect URL>/connectors/ \
 -d '{ 
    "name": "<Unique name for the connector>", 
    "config": 
    { 
        "connector.class": "com.singlestore.debezium.SingleStoreConnector", 
        "tasks.max": "1", 
        "database.hostname": "<SingleStoreDB Hostname>", 
        "database.port": "<SingleStoreDB Port>", 
        "database.user": "<SingleStoreDB User>", 
        "database.password": "<SingleStoreDB Password>", 
        "topic.prefix": "<Topic Preifx>", 
        "database.dbname": "<SingleStoreDB Database>",
        "database.table": "<SingleStoreDB Table>",
        "delete.handling.mode": "none"
    } 
}'
```

## How the SingleStore Debezium connector works
To optimally configure and run the connector, it is essential to understand how the connector performs snapshots, streams change events, determines Kafka topic names, and uses metadata.

SingleStore Debezium connector uses Change Data Capture (CDC) to capture change events. <!--TODO add link to CDC docs-->

The connector does not support handling schema changes. 
You cannot run `ALTER` and `DROP` queries while the `OBSERVE` query is running, specifically during snapshotting and filtering.

### Snapshots
When the connector starts for the first time, it performs an initial consistent snapshot of the database. The default procedure for performing a snapshot consists of the following steps:
1. Establish a connection to the database.
2. Identify the table to be captured.
3. Read the schema of the captured table.
4. Run the `OBSERVE` <!--TODO add link to CDC docs--> query and read everything until the  `CommitSnapshot` event is received for each database partition.
5. Record the successful completion of the snapshot. Offset will include a list of offsets of `CommitSnapshot` events for each database partition.

### Streaming
After the initial snapshot is complete, the connector continues streaming from the offset that it receives in Steps 4 and 5 above. If the streaming stops again for any reason, upon restart, the connector tries to continue streaming changes from where it previously left off.

The SingleStore connector forwards change events in records to the Kafka Connect framework. The Kafka Connect process asynchronously writes the change event records in the same order in which they were generated to the appropriate Kafka topic.
Kafka Connect periodically records the most recent offset in another Kafka topic. The offset indicates source-specific position information that Debezium includes with each event. The connector records database-level offsets for each database partition in each change event.

When Kafka Connect gracefully shuts down, it stops the connectors, flushes all event records to Kafka, and records the last offset received from each connector. When Kafka Connect restarts, it reads the last recorded offset for each connector, and starts each connector at its last recorded offset. When the connector restarts, it sends a request to the SingleStoreDB to send the events that occurred after the offset position.

### Topic names
The SingleStore Debezium connector writes change events for all `INSERT`, `UPDATE`, and `DELETE` operations that occur in a table to a single Apache Kafka topic that is specific to that table. The connector uses the following naming convention for the change event topics:

```
topicPrefix.databaseName.tableName
```
The following list provides definitions for the components of the default name:
| Component      | Description |
|-----           |-----        |
| `topicPrefix`  | Specified by the `topic.prefix` connector configuration property. |
| `databaseName` | The name of the database that contains the table. Specified using the `database.dbname` connector configuration property.        |
| `tableName`    |  The name of the table in which the operation occurred. Specified using the `database.table` connector configuration property.

For example, if the topic prefix is `fulfillment`, database name is `inventory`, and the table where the operation occurred is `orders`, the connector writes events to the `fulfillment.inventory.orders` Kafka topic.

## Data change events
Every data change event that the SingleStore Debezium connector generates has a **key** and a **value**. The structures of the key and value depend on the table from which the change events originate. For information on how Debezium constructs topic names, refer to [Topic names](#topic-names).

Debezium and Kafka Connect are designed around continuous streams of event messages. However, the structure of these events may change over time, which can be difficult for topic consumers to handle. To facilitate the processing of mutable event structures, each event in Kafka Connect is self-contained. Every message key and value has two parts: a schema and payload. The schema describes the structure of the payload, while the payload contains the actual data.

### Change event keys
For each changed table, the change event key payload consists of a single field named `internalid`. It represents a unique ID assigned to each row in the database.

```
{
   "schema":{ (1)
      "type":"struct",
      "optional":false,
      "fields":[
         {
            "type":"int64",
            "optional":false,
            "field":"InternalId"
         }
      ]
   },
   "payload":{ (2)
      "InternalId":1152921504606847003
   }
}
```

| Item | Field name | Description
| -    | -          | -
| 1    | `schema`     | Specifies a Kafka Connect schema that describes the structure of the key's `payload`.
| 2    | `payload`    | Contains a single field `internalId` used to specify the ID of the row for which this change event was generated.

### Change event values

Similar to the key, the value has a `schema` and a `payload`. However, the value in a change event is a bit more complicated than the key. The `schema` describes the Event Envelope structure of the `payload`, including its nested fields. Change events for operations that create, update, or delete data each have a value `payload` with an envelope structure.

An envelope structure in the `payload` sections of a change event value contains the following fields:

 * `op` (Required) -  Contains a string value that describes the type of operation. The `op` field contains one of the following values: `c` (insert), `u` (update), `d` (delete), or `r` (read, which indicates a snapshot).
 * `before` (Optional) - Describes the state of the row before the event occurred. This field is always `null`. It is added to match the format generated by other Debezium connectors.
 * `after` (Optional) - If present, describes the state of a row after a change occurs.
 * `source` (Required) - Contains a structure that describes the source metadata for the event. The structure includes the following fields:
   * `version` - The Debezium version.
   * `connector` - The connector plugin name.
   * `name` - The connector name.
   * `db` - Database name.
   * `table` - Table name.
   * `snapshot` - Whether the event is part of an ongoing snapshot or not.
   * `txId` - The database transaction ID.
   * `partitionId` - The database partition ID.
   * `offsets` - The list of database-level offsets for each database partition.
   * `ts_ms` - The time (based on the system clock in the JVM that runs the Kafka Connect task) at which the connector processed the event. Other Debezium connectors include timestamp that indicates when the record in the source database changed, but it is not possible to retrieve this information using SingleStore CDC.
   * `sequence` - An extra sequencing metadata about a change event (always `null`).
 * `ts_ms` (Optional) - If present, specifies the time (based on the system clock in the JVM that runs the Kafka Connect task) at which the connector processed the event.

### create events
The following example demonstrates the value of a change event that the connector generates for an operation that inserts data in the `t` table:
```
{
   "schema":{...},
   "payload":{
      "before":null,
      "after":{
         "a":33
      },
      "source":{
         "version":"0.1.1",
         "connector":"singlestore",
         "name":"singlestore",
         "ts_ms":1706197043473,
         "snapshot":"true",
         "db":"db",
         "sequence":null,
         "table":"t",
         "txId":"ffffffffffffffff0000000000000003000000000000004600000000000460390000000000000000",
         "partitionId":0,
         "offsets":[
            "000000000000000300000000000000460000000000046049"
         ]
      },
      "op":"c",
      "ts_ms":1706197043473,
      "transaction":null
   }
}
```
### update events
The following example shows an update change event that the connector captures from the same table as the preceding create event.
```
{
   "schema":{...},
   "payload":{
      "before":null,
      "after":{
         "a":22
      },
      "source":{
         "version":"0.1.1",
         "connector":"singlestore",
         "name":"singlestore",
         "ts_ms":1706197446500,
         "snapshot":"true",
         "db":"db",
         "sequence":null,
         "table":"t",
         "txId":"ffffffffffffffff0000000000000003000000000000004c000000000004c16e0000000000000000",
         "partitionId":0,
         "offsets":[
            "0000000000000003000000000000004c000000000004c17e"
         ]
      },
      "op":"u",
      "ts_ms":1706197446500,
      "transaction":null
   }
}
```

The `payload` structure is the same as the create (insert) event, but the following values are different:
 * The value of the `op` field is `u`, signifying that this row changed because of an update.
 * The `after` field shows the updated state of the row, with the `a` value now set to `22`.
 * The structure of the `source` field includes the same fields as before, but the values are different, because the connector captured the event from a different position.
 * The `ts_ms` field shows the timestamp that indicates when Debezium processed the event.

### delete events
The following example shows a delete event for the table that is shown in the preceding create and update event examples.
```
{
   "schema":{...},
   "payload":{
      "before":null,
      "after":null,
      "source":{
         "version":"0.1.1",
         "connector":"singlestore",
         "name":"singlestore",
         "ts_ms":1706197665407,
         "snapshot":"true",
         "db":"db",
         "sequence":null,
         "table":"t",
         "txId":"ffffffffffffffff00000000000000030000000000000053000000000005316e0000000000000000",
         "partitionId":0,
         "offsets":[
            "000000000000000300000000000000530000000000053179"
         ]
      },
      "op":"d",
      "ts_ms":1706197665408,
      "transaction":null
   }
}
```

The `payload` is different when compared to the create or update event:
 * The value of the `op` field is `d`, signifying that the row was deleted.
 * The value of the after field is `null`, signifying that the row no longer exists.
 * The structure of the `source` field includes the same fields as before, but the values are different, because the connector captured the event from a different position.
 * The `ts_ms` field shows the timestamp that indicates when Debezium processed the event.

## Data type mappings
Each change event record is structured in the same way as the original table, where the event record includes a field for each column value. The data type of a table column determines how the connector represents the column’s values in change event fields, as shown in the tables in the following sections.

For each column in a table, Debezium maps the source data type to a literal type, and in some cases, to a semantic type in the corresponding event field.
 * Literal types - Describe how the value is literally represented, using one of the following Kafka Connect schema types: `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT32`, `FLOAT64`, `BOOLEAN`, `STRING`, `BYTES`, `ARRAY`, `MAP`, and `STRUCT`.
 * Semantic types - Describe how the Kafka Connect schema captures the meaning of the field, by using the name of the Kafka Connect schema for the field.

### Integer numbers
| SingleStoreDB data type | Literal type
| -                       | -
| BOOL                    | INT16
| BIT                     | BYTES
| TINYINT                 | INT16
| SMALLINT                | INT16
| MEDIUMINT               | INT32
| INT                     | INT32
| BIGINT                  | INT64

> Note: `BIT` type is returned in reversed order.

### Real numbers
| SingleStoreDB data type | Literal type
| -                       | -           
| FLOAT                   | FLOAT32     
| DOUBLE                  | FLOAT64     

Debezium connectors handle `DECIMAL` based on the value of [`decimal.handling.mode`](#decimal-handling-mode) connector configuration property.

| `decimal.handling.mode` | Literal type | Semantic type
| -                       | -            | -
| precise (default)       | BYTES        | `org.apache.kafka.connect.data.Decimal` - The scale schema parameter contains an integer that specifies the number of places the decimal point is shifted.
| double                  | FLOAT64      | n/a
| string                  | STRING       | n/a

### Time and date
Time and date types depend on the value of the [`time.precision.mode`](#time-precision-mode) connector configuration property. 
`time.precision.mode=adaptive_time_microseconds(default)`
| SingleStoreDB data type | Literal type | Semantic type
| -                       | -            | -
| DATE                    | INT32        | `io.debezium.time.Date` - Represents the number of days since the epoch.
| TIME                    | INT64        | `io.debezium.time.MicroTime` - Represents the time value in microseconds.
| TIME(6)                 | INT64        | `io.debezium.time.MicroTime` - Represents the time value in microseconds.
| DATETIME                | INT64        | `io.debezium.time.Timestamp` - Represents the number of milliseconds past the epoch.
| DATETIME(6)             | INT64        | `io.debezium.time.MicroTimestamp` - Represents the number of microseconds past the epoch.
| TIMESTAMP               | INT64        | `io.debezium.time.Timestamp` - Represents the number of milliseconds past the epoch.
| TIMESTAMP(6)            | INT64        | `io.debezium.time.MicroTimestamp` - Represents the number of microseconds past the epoch.
| YEAR                    | INT32        | N/A

`time.precision.mode=connect`
| SingleStoreDB data type | Literal type | Semantic type
| -                       | -            | -
| DATE                    | INT32        | `org.apache.kafka.connect.data.Date` - Represents the number of days since the epoch.
| TIME                    | INT64        | `org.apache.kafka.connect.data.Time` - Represents the time value in milliseconds.
| TIME(6)                 | INT64        | `org.apache.kafka.connect.data.Time` - Represents the time value in milliseconds.
| DATETIME                | INT64        | `org.apache.kafka.connect.data.Timestamp` - Represents the number of milliseconds since the epoch.
| DATETIME(6)             | INT64        | `org.apache.kafka.connect.data.Timestamp` - Represents the number of milliseconds since the epoch.
| TIMESTAMP               | INT64        | `org.apache.kafka.connect.data.Timestamp` - Represents the number of milliseconds since the epoch.
| TIMESTAMP(6)            | INT64        | `org.apache.kafka.connect.data.Timestamp` - Represents the number of milliseconds since the epoch.
| YEAR                    | INT32        | N/A

### String types
| SingleStoreDB data type | Literal type
| -                       | -
| TINYTEXT                | STRING
| TEXT                    | STRING
| MEDIUMTEXT              | STRING
| LONGTEXT                | STRING
| CHAR                    | STRING
| VARCHAR                 | STRING

### Blob types
Debezium connectors handle binary data types based on the value of the [`binary.handling.mode`](#binary-handling-mode) connector configuration property.
| SingleStoreDB data type | Literal type    | Semantic type
| -                       | -               | -
| TINYBLOB                | BYTES OR STRING | Either the raw bytes (the default), a base64-encoded String, or a base64-url-safe-encoded String, or a hex-encoded String, based on the [binary.handling.mode](#binary-handling-mode) connector configuration property.
| BLOB                    | BYTES OR STRING | Either the raw bytes (the default), a base64-encoded String, or a base64-url-safe-encoded String, or a hex-encoded String, based on the [binary.handling.mode](#binary-handling-mode) connector configuration property.
| MEDIUMBLOB              | BYTES OR STRING | Either the raw bytes (the default), a base64-encoded String, or a base64-url-safe-encoded String, or a hex-encoded String, based on the [binary.handling.mode](#binary-handling-mode) connector configuration property.
| LONGBLOB                | BYTES OR STRING | Either the raw bytes (the default), a base64-encoded String, or a base64-url-safe-encoded String, or a hex-encoded String, based on the [binary.handling.mode](#binary-handling-mode) connector configuration property.
| BINARY                  | BYTES OR STRING | Either the raw bytes (the default), a base64-encoded String, or a base64-url-safe-encoded String, or a hex-encoded String, based on the [binary.handling.mode](#binary-handling-mode) connector configuration property.
| VARBINARY               | BYTES OR STRING | Either the raw bytes (the default), a base64-encoded String, or a base64-url-safe-encoded String, or a hex-encoded String, based on the [binary.handling.mode](#binary-handling-mode) connector configuration property.

### Other types
| SingleStoreDB data type | Literal type    | Semantic type
| -                       | -               | -
| JSON                    | STRING          | N/A
| ENUM                    | STRING          | N/A
| SET                     | STRING          | N/A
| GEOGRAPHYPOINT          | STRUCT          | `io.debezium.data.geometry.Geometry` - Contains a structure with two fields: `srid` (INT32): spatial reference system ID that defines the type of geometry object stored in the structure and `wkb` (BYTES): binary representation of the geometry object encoded in the Well-Known-Binary (wkb) format. Refer to the [Open Geospatial Consortium](https://www.opengeospatial.org/standards/sfa) for more details.
| GEOGRAPHY               | STRUCT          | `io.debezium.data.geometry.Geometry` - Contains a structure with two fields: `srid` (INT32): spatial reference system ID that defines the type of geometry object stored in the structure and `wkb` (BYTES): binary representation of the geometry object encoded in the Well-Known-Binary (wkb) format. Refer to the [Open Geospatial Consortium](https://www.opengeospatial.org/standards/sfa) for more details.

## Connector properties
The SingleStore Debezium connector supports the following configuration properties which can be used to achieve the right connector behavior for your application. 

### Kafka connect properties
| Property                                | Default    | Description
| -                                       | -          | -
| name                                    |  | Unique name for the connector. Any attempts to register again with the same name will fail. This property is required by all Kafka Connect connectors.
| connector.class                         |  | The name of the Java Class for the connector. For the SingleStore connector specify `com.singlestore.debezium.SingleStoreConnector`.
| tasks.max                               | 1          | The maximum number of tasks that can be created for this connector. The SingleStoreDB connector only uses a single task and fails if more is specified. Hence, the default is always acceptable.

### Connection properties
| Property                                | Default    | Description
| -                                       | -          | -
| database.hostname                       |            | Resolvable hostname or IP address of the database server.
| database.port                           | 3306       | Port of the database server.
| database.user                           |            | Name of the database user to be used when connecting to the database.
| database.password                       |            | Password of the database user to be used when connecting to the database.
| database.dbname                         |            | The name of the database from which the connector should capture changes.
| database.table                          |            | The name of the table from which the connector should capture changes.
| database.ssl.mode                       | disable    | Whether to use an encrypted connection to SingleStore. Options include: 'disable' to use an unencrypted connection (the default), 'trust' to use a secure (encrypted) connection (no certificate and hostname validation), 'verify_ca' to use a secure (encrypted) connection but additionally verify the server TLS certificate against the configured Certificate Authority (CA) certificates, or fail if no valid matching CA certificates are found, or 'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.
| database.ssl.keystore                   |            | The location of the key store file. This is optional and can be used for two-way authentication between the client and the SingleStore server.
| database.ssl.keystore.password          |            | The password for the key store file. This is optional and only needed if 'database.ssl.keystore' is configured.
| database.ssl.truststore                 |            | The location of the trust store file for the server certificate verification.
| database.ssl.truststore.password        |            | The password for the trust store file. Used to check the integrity of the truststore and unlock the truststore.
| database.ssl.server.cert                |            | Server's certificate in DER format or the server's CA certificate. The certificate is added to the trust store, which allows the connection to trust a self-signed certificate.
| connect.timeout.ms                      | 30000      | Maximum time to wait after trying to connect to the database before timing out, specified in milliseconds.
| driver.parameters                       |               | Additional JDBC parameters to use in the connection string to connect to the SingleStore server in the following format: 'param1=value1; param2 = value2; ...'. Refer to [SingleStore Connection String Parameters](https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters) for more information.

### Required connector configuration properties
The following configuration properties are required unless a default value is available.

| Property                                | Default       | Description
| -                                       | -             | -
| topic.prefix                            |               | Specifies the topic prefix that identifies and provides a namespace for the particular database server/cluster that is capturing the changes. The topic prefix should be unique across all other connectors, since it is used as a prefix for all Kafka topic names that receive events generated by this connector. Only alphanumeric characters, hyphens, dots, and underscores are accepted.
| <span id="decimal-handling-mode">decimal.handling.mode</span>                   | precise       | Specifies how DECIMAL and NUMERIC columns are represented in change events. Values include: 'precise' - uses `java.math.BigDecimal` to represent values, which are encoded in the change events using a binary representation and Kafka Connect's 'org.apache.kafka.connect.data.Decimal' type; 'string' - uses string to represent values; 'double' - represents values using Java's 'double', which may not offer the precision, but it is easier to use in consumers.
| <span id="binary-handling-mode">binary.handling.mode</span> | bytes | Specifies how binary (blob, binary, etc.) columns are represented in change events. Values include: 'bytes' - represents binary data as byte array (default); 'base64' - represents binary data as base64-encoded string; 'base64-url-safe' - represents binary data as base64-url-safe-encoded string; 'hex' - represents binary data as hex-encoded (base16) string.
| <span id="time-precision-mode">time.precision.mode</span>                     | advanced      | Specifies the precision type for time, date, and timestamps. Values include: 'adaptive' - bases the precision of time, date, and timestamp values on the database column's precision; 'adaptive_time_microseconds' - similar to 'adaptive' mode, but TIME fields always use microseconds precision; 'connect' - always represents time, date, and timestamp values using Kafka Connect's built-in representations for Time, Date, and Timestamp, which uses millisecond precision regardless of the database columns' precision.
| tombstones.on.delete                    | true          | Whether delete operations should be represented by a delete event and a subsequent tombstone event ('true') or only by a delete event ('false'). Generating the tombstone event (the default behavior) allows Kafka to completely delete all events pertaining to the given key once the source record is deleted.
| column.include.list                     |               | Regular expressions matching columns to include in change events.
| column.exclude.list                     |               | Regular expressions matching columns to exclude from change events.
| column.mask.hash.([^.]+).with.salt.(.+) |               | A comma-separated list of regular expressions matching fully-qualified names of columns that should be masked by hashing the input, using the specified hash algorithms and salt.
| column.mask.with.(d+).chars             |               | A comma-separated list of regular expressions matching fully-qualified names of columns that should be masked with the specified number of asterisks ('*').
| column.truncate.to.(d+).chars           |               | A comma-separated list of regular expressions matching fully-qualified names of columns that should be truncated to the configured amount of characters.
| column.propagate.source.type            |               | A comma-separated list of regular expressions matching fully-qualified names of columns that adds the column’s original type and original length as parameters to the corresponding field schemas in the emitted change records.
| datatype.propagate.source.type          |               | A comma-separated list of regular expressions matching the database-specific data type names that adds the data type's original type and original length as parameters to the corresponding field schemas in the generated change records.
| populate.internal.id                    | false         | Specifies whether to add `internalId` to the `after` field of the event message.

### Advanced connector configuration properties
The following advanced configuration properties have defaults that work in most situations and therefore rarely need to be specified in the connector’s configuration.

| Property                                | Default       | Description
| -                                       | -             | -
| converters                              |               | Optional list of custom converters to use instead of default ones. The converters are defined using the `<converter.prefix>.type` option and configured using `<converter.prefix>.<option>`.
| snapshot.mode                           | initial       | Specifies the snapshot strategy to use on connector startup. Supported modes: 'initial' (default) - If the connector does not detect any offsets for the logical server name, it performs a full snapshot that captures the current state of the configured tables. After the snapshot completes, the connector begins to stream changes.; 'initial_only' - Similar to the 'initial' mode, the connector performs a full snapshot. Once the snapshot is complete, the connector stops, and does not stream any changes.
| event.processing.failure.handling.mode  | fail          | Specifies how failures that may occur during event processing should be handled, for example, failures because of a corrupted event. Supported modes: 'fail' (Default) - An exception indicating the problematic event and its position is raised and the connector is stopped; 'warn' - The problematic event and its position are logged and the event is skipped; 'ignore' - The problematic event is skipped.
| max.batch.size                          | 2048          | Maximum size of each batch of source records.
| max.queue.size                          | 8192          | Maximum size of the queue for change events read from the database log but not yet recorded or forwarded.
| max.queue.size.in.bytes                 | 0 (disabled)  | Maximum size of the queue in bytes for change events read from the database log but not yet recorded or forwarded.
| poll.interval.ms                        | 500           | Time (in milliseconds) that the connector waits for new change events to appear after receiving no events.
| heartbeat.topics.prefix                 | __debezium-heartbeat | The prefix that is used to name heartbeat topics.
| heartbeat.action.query                  |            | An optional query to execute with every heartbeat.
| heartbeat.interval.ms                   | 0 (disabled)  | The time interval in milliseconds at which the connector periodically sends heartbeat messages to a heartbeat topic.
| snapshot.delay.ms                       | 0             | Number of milliseconds to wait before a snapshot begins.
| retriable.restart.connector.wait.ms     | 10000         | Number of milliseconds to wait before restarting connector after a retriable exception occurs.
| skipped.operations                      | t             | The comma-separated list of operations to skip during streaming, defined as: 'c' for inserts/create; 'u' for updates; 'd' for deletes, 't' for truncates, and 'none' to indicate that nothing is skipped.
| notification.enabled.channels           |               | List of notification channels names that are enabled.
| topic.naming.strategy                   |               | The name of the TopicNamingStrategy Class that should be used to determine the topic name for data change, schema change, transaction, heartbeat event, etc.
| custom.metric.tags                      |               | The custom metric tags will accept key-value pairs to customize the MBean object name which should be appended at the end of the regular name. Each key represents a tag for the MBean object name, and the corresponding value represents the value of that key. For example: k1=v1,k2=v2.
| errors.max.retries                      | -1 (no limit) | The maximum number of retries on connection errors before failing.
| sourceinfo.struct.maker                 | SingleStoreSourceInfoStructMaker | The name of the SourceInfoStructMaker Class that returns the SourceInfo schema and struct.
| notification.sink.topic.name            |               | The name of the topic for the notifications. This property is required if the 'sink' is in the list of enabled channels.
| post.processors                         |               | Optional list of post processors. The processors are defined using the `<post.processor.prefix>.type` option and configured using `<post.processor.prefix.<option>`.
