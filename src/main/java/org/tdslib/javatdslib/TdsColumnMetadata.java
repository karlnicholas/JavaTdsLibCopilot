package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

class TdsColumnMetadata implements ColumnMetadata {
  private final ColumnMeta meta;

  TdsColumnMetadata(ColumnMeta meta) {
    this.meta = meta;
  }

  @Override public String getName() { return meta.getName(); }
  @Override public Integer getPrecision() { return meta.getMaxLength(); }
  @Override public Integer getScale() { return (int) meta.getScale(); }
  @Override public Object getNativeTypeMetadata() { return meta; }

  @Override
  public Nullability getNullability() {
    // TDS Flags bit 0x01 indicates Nullable
    return (meta.getFlags() & 0x01) != 0 ? Nullability.NULLABLE : Nullability.NON_NULL;
  }
}