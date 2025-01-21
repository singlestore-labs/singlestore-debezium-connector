package com.singlestore.debezium.util;

public class Utils {

  public static String bytesToHex(byte[] bytes) {
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

  public static String escapeString(String s) {
    return "'" + s.replace("'", "''") + "'";
  }

  /**
   * Get original type name. Here original type name is part of the name before "(" For example,
   * "VECTOR(3, I32)" -> "VECTOR"
   *
   * @param type - type name returned by the database
   * @return part of the name before "("
   */
  public static String getOriginalTypeName(String type) {
    type = type.toUpperCase();

    int i = type.indexOf("(");
    if (i != -1) {
      return type.substring(0, i);
    } else {
      return type;
    }
  }
}
