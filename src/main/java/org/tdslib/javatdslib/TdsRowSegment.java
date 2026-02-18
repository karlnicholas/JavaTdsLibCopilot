package org.tdslib.javatdslib;

import io.r2dbc.spi.*;
import java.util.*;

// 1. Row Segment (Unchanged)
class TdsRowSegment implements Result.RowSegment {
  private final Row row;
  TdsRowSegment(Row row) { this.row = row; }
  @Override public Row row() { return row; }
}

// 2. Update Count Segment (Unchanged)
class TdsUpdateCount implements Result.UpdateCount {
  private final long value;
  TdsUpdateCount(long value) { this.value = value; }
  @Override public long value() { return value; }
}

// 3. Out Parameters Segment (Unchanged)
class TdsOutSegment implements Result.OutSegment {
  private final OutParameters outParameters;
  TdsOutSegment(OutParameters outParameters) { this.outParameters = outParameters; }
  @Override public OutParameters outParameters() { return outParameters; }
}

// 4. Standalone OutParameters Implementation
class TdsOutParameters implements OutParameters {
  private final List<Object> values;
  private final TdsOutParametersMetadata metadata;
  private final Map<String, Integer> nameToIndex;

  // UPDATED: Constructor now takes List<TdsOutParameterMetadata>
  TdsOutParameters(List<Object> values, List<TdsOutParameterMetadata> metadataList) {
    this.values = values;
    this.metadata = new TdsOutParametersMetadata(metadataList);
    this.nameToIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    for (int i = 0; i < metadataList.size(); i++) {
      String name = metadataList.get(i).getName();
      if (name != null) {
        nameToIndex.put(name, i);
        if (name.startsWith("@")) {
          nameToIndex.put(name.substring(1), i);
        }
      }
    }
  }

  @Override
  public OutParametersMetadata getMetadata() {
    return metadata;
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    if (index < 0 || index >= values.size()) {
      throw new IllegalArgumentException("Invalid OutParameter Index: " + index);
    }
    return convert(values.get(index), type);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    Integer index = nameToIndex.get(name);
    if (index == null) {
      throw new IllegalArgumentException("OutParameter not found: " + name);
    }
    return get(index, type);
  }

  @SuppressWarnings("unchecked")
  private <T> T convert(Object value, Class<T> targetType) {
    if (value == null) return null;
    if (targetType.isInstance(value)) return (T) value;
    if (targetType == Long.class && value instanceof Integer) return (T) Long.valueOf(((Integer) value).longValue());
    if (targetType == Double.class && value instanceof Float) return (T) Double.valueOf(((Float) value).doubleValue());
    if (targetType == String.class) return (T) value.toString();
    throw new IllegalArgumentException("Cannot convert " + value.getClass().getSimpleName() + " to " + targetType.getSimpleName());
  }
}