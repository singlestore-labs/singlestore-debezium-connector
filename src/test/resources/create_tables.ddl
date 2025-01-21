CREATE TABLE IF NOT EXISTS db.person (
    name VARCHAR(255) primary key,
    birthdate DATE NULL,
    age INTEGER NULL DEFAULT 10,
    salary DECIMAL(5,2),
    bitStr BIT(18),
    sort key(name)
);
CREATE TABLE IF NOT EXISTS db.product (
    id INT NOT NULL AUTO_INCREMENT,
    createdByDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modifiedDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY(id)
);
CREATE TABLE IF NOT EXISTS db.purchased (
    purchaser VARCHAR(255) NOT NULL,
    productId INT NOT NULL,
    purchaseDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(productId,purchaser)
);
CREATE TABLE IF NOT EXISTS db.song (
    author TEXT,
    name TEXT
);

CREATE ROWSTORE TABLE IF NOT EXISTS db.allTypesTable (
    `boolColumn` BOOL DEFAULT true,
    `booleanColumn` BOOLEAN DEFAULT true,
    `bitColumn` BIT(64) DEFAULT '01234567',
    `tinyintColumn` TINYINT DEFAULT 124,
    `mediumintColumn` MEDIUMINT DEFAULT 8388607,
    `smallintColumn` SMALLINT DEFAULT 32767,
    `intColumn` INT DEFAULT 2147483647,
    `integerColumn` INTEGER DEFAULT 2147483647,
    `bigintColumn` BIGINT DEFAULT 9223372036854775807,
    `floatColumn` FLOAT DEFAULT 10.1,
    `doubleColumn` DOUBLE DEFAULT 100.1,
    `realColumn` REAL DEFAULT 100.1,
    `dateColumn` DATE DEFAULT '2000-10-10',
    `timeColumn` TIME DEFAULT '22:59:59',
    `time6Column` TIME(6) DEFAULT '22:59:59.111111',
    `datetimeColumn` DATETIME DEFAULT '2023-12-31 23:59:59',
    `datetime6Column` DATETIME(6) DEFAULT '2023-12-31 22:59:59.111111',
    `timestampColumn` TIMESTAMP DEFAULT '2022-01-19 03:14:07',
    `timestamp6Column` TIMESTAMP(6) DEFAULT '2022-01-19 03:14:07.111111',
    `yearColumn` YEAR DEFAULT '1989',
    `decimalColumn` DECIMAL(65, 30) DEFAULT 10000.100001,
    `decColumn` DEC DEFAULT 10000,
    `fixedColumn` FIXED DEFAULT 10000,
    `numericColumn` NUMERIC DEFAULT 10000,
    `charColumn` CHAR DEFAULT 'a',
    `mediumtextColumn` MEDIUMTEXT DEFAULT 'abc',
    `binaryColumn` BINARY DEFAULT 'a',
    `varcharColumn` VARCHAR(100) DEFAULT 'abc',
    `varbinaryColumn` VARBINARY(100) DEFAULT 'abc',
    `longtextColumn` LONGTEXT DEFAULT 'abc',
    `textColumn` TEXT DEFAULT 'abc',
    `tinytextColumn` TINYTEXT DEFAULT 'abc',
    `longblobColumn` LONGBLOB DEFAULT 'abc',
    `mediumblobColumn` MEDIUMBLOB DEFAULT 'abc',
    `blobColumn` BLOB DEFAULT 'abc',
    `tinyblobColumn` TINYBLOB DEFAULT 'abc',
    `jsonColumn` JSON DEFAULT '{}',
    `enum_f` ENUM('val1','val2','val3') default 'val1',
    `set_f` SET('v1','v2','v3') default 'v1',
    `geographyColumn` GEOGRAPHY DEFAULT 'POLYGON((1 1,2 1,2 2, 1 2, 1 1))',
    `geographypointColumn` GEOGRAPHYPOINT DEFAULT 'POINT(1.50000003 1.50000000)',
    `bsonColumn` BSON DEFAULT 0x0500000000,
    `vectorI8Column` VECTOR(3, I8) DEFAULT '[2, 10, 100]',
    `vectorI16Column` VECTOR(3, I16) DEFAULT '[2, 10, 100]',
    `vectorI32Column` VECTOR(3, I32) DEFAULT '[2, 10, 100]',
    `vectorI64Column` VECTOR(3, I64) DEFAULT '[2, 10, 100]',
    `vectorF32Column` VECTOR(3, F32) DEFAULT '[2.1, 10.1, 100.1]',
    `vectorF64Column` VECTOR(3, F64) DEFAULT '[2.1, 10.1, 100.1]',
     unique key(intColumn),
     shard key(intColumn)
 );
