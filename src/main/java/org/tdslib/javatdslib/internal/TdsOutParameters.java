package org.tdslib.javatdslib.internal;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.tdslib.javatdslib.codec.DecoderRegistry;
import org.tdslib.javatdslib.protocol.TdsType;

/**
 * Implementation of {@link OutParameters} for the TDS protocol. This class holds the output
 * parameters returned from a stored procedure execution, providing access to their values and
 * metadata.
 */
public class TdsOutParameters implements OutParameters {
  private final List<byte[]> rawValues;
  private final TdsOutParametersMetadata metadata;
  private final List<TdsOutParameterMetadata> metadataList;
  private final Map<String, Integer> nameToIndex;
  private final java.nio.charset.Charset varcharCharset; // <-- Add this

  /**
   * Constructs a new TdsOutParameters instance.
   *
   * @param rawValues The raw byte values of the output parameters.
   * @param metadataList The metadata for each output parameter.
   * @param varcharCharset The charset used for decoding VARCHAR parameters.
   */
  public TdsOutParameters(
      List<byte[]> rawValues,
      List<TdsOutParameterMetadata> metadataList,
      java.nio.charset.Charset varcharCharset) {
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
    if (data == null) {
      return null;
    }

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
