package com.singlestore.debezium.util;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public final class ObserveResultSetUtils {

    private static final String[] METADATA_COLUMNS = new String[]{"Offset", "PartitionId", "Type", "Table", "TxId", "TxPartitions", "InternalId"};
    private static final String BEGIN_SNAPSHOT = "BeginSnapshot";
    private static final String COMMIT_SNAPSHOT = "CommitSnapshot";

    public static ColumnArray toArray(ResultSet resultSet, Table table) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        int allColumnsCount = metaData.getColumnCount();
        int dataColumnsCount = allColumnsCount - METADATA_COLUMNS.length;
        int firstColumnIndex = allColumnsCount - dataColumnsCount + 1;
        Column[] columns = new Column[dataColumnsCount];
        int greatestColumnPosition = 0;
        for (int i = 0; i < dataColumnsCount; i++) {
            final String columnName = metaData.getColumnName(i + firstColumnIndex);
            columns[i] = table.columnWithName(columnName);
            if (columns[i] == null) {
                // This situation can happen when SQL Server and Db2 schema is changed before
                // an incremental snapshot is started and no event with the new schema has been
                // streamed yet.
                // This warning will help to identify the issue in case of a support request.

                final String[] resultSetColumns = new String[metaData.getColumnCount()];
                for (int j = 0; j < metaData.getColumnCount(); j++) {
                    resultSetColumns[j] = metaData.getColumnName(j + firstColumnIndex);
                }
                throw new IllegalArgumentException("Column '"
                        + columnName
                        + "' not found in result set '"
                        + String.join(", ", resultSetColumns)
                        + "' for table '"
                        + table.id()
                        + "', "
                        + table
                        + ". This might be caused by DBZ-4350");
            }
            greatestColumnPosition = greatestColumnPosition < columns[i].position()
                    ? columns[i].position()
                    : greatestColumnPosition;
        }
        return new ColumnArray(columns, greatestColumnPosition);
    }

    public static Object[] rowToArray(Table table, ResultSet rs, ColumnArray columnArray) throws SQLException {
        final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
        for (int i = 0; i < columnArray.getColumns().length; i++) {
            row[columnArray.getColumns()[i].position() - 1] = getColumnValue(rs, i + 1);
        }
        return row;
    }

    public static Object getColumnValue(ResultSet rs, int columnIndex) throws SQLException {
        return rs.getObject(columnIndex + METADATA_COLUMNS.length);
    }

    public static class ColumnArray {

        private Column[] columns;
        private int greatestColumnPosition;

        public ColumnArray(Column[] columns, int greatestColumnPosition) {
            this.columns = columns;
            this.greatestColumnPosition = greatestColumnPosition;
        }

        public Column[] getColumns() {
            return columns;
        }

        public int getGreatestColumnPosition() {
            return greatestColumnPosition;
        }
    }

    public static String offset(ResultSet rs) throws SQLException {
        return bytesToHex(rs.getBytes(METADATA_COLUMNS[0]));
    }

    public static Integer partitionId(ResultSet rs) throws SQLException {
        return rs.getInt(METADATA_COLUMNS[1]) - 1;
    }

    private static String bytesToHex(byte[] bytes) {
        char[] res = new char[bytes.length*2];

        int j = 0;
        for (int i = 0; i < bytes.length; i++) {
            res[j++] = Character.forDigit((bytes[i] >> 4) & 0xF, 16);
            res[j++] = Character.forDigit((bytes[i] & 0xF), 16);
        }

        return new String(res);
    }

    public static boolean isBeginSnapshot(ResultSet rs) throws SQLException {
        return BEGIN_SNAPSHOT.equals(snapshotType(rs));
    }

    public static boolean isCommitSnapshot(ResultSet rs) throws SQLException {
        return COMMIT_SNAPSHOT.equals(snapshotType(rs));
    }

    public static String snapshotType(ResultSet rs) throws SQLException {
        return rs.getString(METADATA_COLUMNS[2]);
    }

    public static String tableName(ResultSet rs) throws SQLException {
        return rs.getString(METADATA_COLUMNS[3]);
    }

    public static String txId(ResultSet rs) throws SQLException {
        return bytesToHex(rs.getBytes(METADATA_COLUMNS[4]));
    }

    public static String txPartitions(ResultSet rs) throws SQLException {
        return rs.getString(METADATA_COLUMNS[5]);
    }

    public static Long internalId(ResultSet rs) throws SQLException {
        return rs.getLong(METADATA_COLUMNS[6]);
    }

    private ObserveResultSetUtils() {
    }
}
