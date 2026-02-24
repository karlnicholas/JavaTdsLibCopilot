package org.tdslib.javatdslib;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
  private final List<byte[]> rawValues;
  private final TdsOutParametersMetadata metadata;
  private final List<TdsOutParameterMetadata> metadataList;
  private final Map<String, Integer> nameToIndex;
  private final java.nio.charset.Charset varcharCharset; // <-- Add this

  TdsOutParameters(List<byte[]> rawValues, List<TdsOutParameterMetadata> metadataList, java.nio.charset.Charset varcharCharset) {
    this.rawValues = rawValues;
    this.metadataList = metadataList;
    this.metadata = new TdsOutParametersMetadata(metadataList);
    this.nameToIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    this.varcharCharset = varcharCharset; // <-- Assign it here

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
    if (index < 0 || index >= rawValues.size()) {
      throw new IllegalArgumentException("Invalid OutParameter Index: " + index);
    }

    byte[] data = rawValues.get(index);
    if (data == null) return null;

    TdsOutParameterMetadata meta = metadataList.get(index);
    TdsType tdsType = meta.getTdsType();
    int scale = meta.getScale() != null ? meta.getScale() : 0;

    return TdsDataConverter.convert(data, tdsType, type, scale, this.varcharCharset);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    Integer index = nameToIndex.get(name);
    if (index == null) {
      throw new IllegalArgumentException("OutParameter not found: " + name);
    }
    return get(index, type);
  }
}