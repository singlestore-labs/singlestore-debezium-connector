package com.singlestore.debezium.util;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Field;

import com.singlestore.debezium.SingleStoreTableSchemaBuilder;

public final class ObserveResultSetUtils {

  private static final String[] METADATA_COLUMNS = new String[]{"Offset", "PartitionId", "Type",
      "Table", "TxId", "TxPartitions", "InternalId"};
  private static final String BEGIN_SNAPSHOT = "BeginSnapshot";
  private static final String COMMIT_SNAPSHOT = "CommitSnapshot";

  public static List<Integer> columnPositions(ResultSet resultSet, List<Column> columns)
      throws SQLException {
    List<Integer> positions = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      String columnName = columns.get(i).name();
      positions.add(resultSet.findColumn(columnName));
    }

    return positions;
  }

  public static Object[] rowToArray(ResultSet rs, List<Integer> positions,
      Boolean populateInternalId) throws SQLException {
    final Object[] row;
    if (populateInternalId) {
      row = new Object[positions.size() + 1];
    } else {
      row = new Object[positions.size()];
    }

    for (int i = 0; i < positions.size(); i++) {
      row[i] = rs.getObject(positions.get(i));
    }
    if (populateInternalId) {
      row[positions.size()] = internalId(rs);
    }

    return row;
  }

  public static String offset(ResultSet rs) throws SQLException {
    return bytesToHex(rs.getBytes(METADATA_COLUMNS[0]));
  }

  public static Integer partitionId(ResultSet rs) throws SQLException {
    return rs.getInt(METADATA_COLUMNS[1]);
  }

  private static String bytesToHex(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    char[] res = new char[bytes.length * 2];

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

  public static String internalId(ResultSet rs) throws SQLException {
    return bytesToHex(rs.getBytes(METADATA_COLUMNS[6]));
  }

  private ObserveResultSetUtils() {
  }
}
