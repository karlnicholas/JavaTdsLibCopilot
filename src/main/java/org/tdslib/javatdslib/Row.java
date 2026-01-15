package org.tdslib.javatdslib;

import java.util.List;

public record Row(List<Byte[]> data) {
  <T> T get(String columnName, Class<T> type) {
    return (T)null;
  };
  <T> T get(int columnIndex, Class<T> type) {
    return (T)null;
  };
}
