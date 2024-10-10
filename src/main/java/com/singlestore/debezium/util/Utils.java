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
}
