package org.tdslib.javatdslib.api;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import org.tdslib.javatdslib.codec.DecoderRegistry;
import org.tdslib.javatdslib.protocol.TdsType;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// 4. Standalone OutParameters Implementation
public class TdsOutParameters implements OutParameters {
  private final List<byte[]> rawValues;
  private final TdsOutParametersMetadata metadata;
  private final List<TdsOutParameterMetadata> metadataList;
  private final Map<String, Integer> nameToIndex;
  private final java.nio.charset.Charset varcharCharset; // <-- Add this

  public TdsOutParameters(List<byte[]> rawValues, List<TdsOutParameterMetadata> metadataList, java.nio.charset.Charset varcharCharset) {
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

    // Use the new Registry
    return DecoderRegistry.DEFAULT.decode(data, tdsType, type, scale, this.varcharCharset);
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
