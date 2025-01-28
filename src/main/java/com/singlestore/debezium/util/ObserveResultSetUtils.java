package com.singlestore.debezium.util;

import static java.sql.Types.TIME;

import com.singlestore.debezium.SingleStoreValueConverters.VectorMode;
import io.debezium.relational.Column;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
      Boolean populateInternalId, VectorMode vectorMode) throws SQLException {
    final Object[] row;
    if (populateInternalId) {
      row = new Object[positions.size() + 1];
    } else {
      row = new Object[positions.size()];
    }
    ResultSetMetaData md = rs.getMetaData();

    for (int i = 0; i < positions.size(); i++) {
      int position = positions.get(i);
      if (md.getColumnType(position) == TIME) {
        row[i] = rs.getTimestamp(position);
      } else if (Utils.getOriginalTypeName(md.getColumnTypeName(position)).equals("VECTOR")
          && vectorMode == VectorMode.BINARY) {
        row[i] = rs.getBytes(position);
      } else {
        row[i] = rs.getObject(position);
      }
    }
    if (populateInternalId) {
      row[positions.size()] = internalId(rs);
    }

    return row;
  }

  public static String offset(ResultSet rs) throws SQLException {
    return Utils.bytesToHex(rs.getBytes(METADATA_COLUMNS[0]));
  }

  public static Integer partitionId(ResultSet rs) throws SQLException {
    return rs.getInt(METADATA_COLUMNS[1]);
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
    return Utils.bytesToHex(rs.getBytes(METADATA_COLUMNS[4]));
  }

  public static String txPartitions(ResultSet rs) throws SQLException {
    return rs.getString(METADATA_COLUMNS[5]);
  }

  public static String internalId(ResultSet rs) throws SQLException {
    return Utils.bytesToHex(rs.getBytes(METADATA_COLUMNS[6]));
  }
}
