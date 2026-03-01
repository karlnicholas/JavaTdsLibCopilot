package org.tdslib.javatdslib.api;

import io.r2dbc.spi.OutParameterMetadata;
import io.r2dbc.spi.OutParametersMetadata;

import java.util.Collections;
import java.util.List;

class TdsOutParametersMetadata implements OutParametersMetadata {
  // Now holding the correct implementation type
  private final List<TdsOutParameterMetadata> metadatas;

  TdsOutParametersMetadata(List<TdsOutParameterMetadata> metadatas) {
    this.metadatas = metadatas;
  }

  @Override
  public OutParameterMetadata getParameterMetadata(int index) {
    if (index < 0 || index >= metadatas.size()) {
      throw new IllegalArgumentException("Invalid index: " + index);
    }
    return metadatas.get(index);
  }

  @Override
  public OutParameterMetadata getParameterMetadata(String name) {
    for (TdsOutParameterMetadata meta : metadatas) {
      if (name.equalsIgnoreCase(meta.getName()) ||
              (meta.getName() != null && meta.getName().equalsIgnoreCase("@" + name))) {
        return meta;
      }
    }
    throw new IllegalArgumentException("Parameter metadata not found for: " + name);
  }

  @Override
  public List<? extends OutParameterMetadata> getParameterMetadatas() {
    return Collections.unmodifiableList(metadatas);
  }
}