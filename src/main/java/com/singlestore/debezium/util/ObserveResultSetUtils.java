package com.singlestore.debezium.util;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public final class ObserveResultSetUtils {

    private static final String[] METADATA_COLUMNS = new String[]{"Offset", "PartitionId", "Type", "Table", "TxId", "TxPartitions", "InternalId"};
    private static final String BEGIN_SNAPSHOT = "BeginSnapshot";
    private static final String COMMIT_SNAPSHOT = "CommitSnapshot";

    public static Object[] rowToArray(ResultSet rs, List<Field> fields) throws SQLException {
        final Object[] row = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            row[i] = rs.getObject(fields.get(i).name());
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
