package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Type;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

public class TdsColumnMetadata implements ColumnMetadata {
  private final String name;
  private final int precision;
  private final int scale;
  private final TdsType tdsType;
  private final Object nativeMeta;

  // Constructor for Row Data (Standard Result Set)
  TdsColumnMetadata(ColumnMeta meta) {
    this.name = meta.getName();
    this.precision = meta.getMaxLength();
    this.scale = meta.getScale();
    this.tdsType = TdsType.valueOf(meta.getDataType());
    this.nativeMeta = meta;
  }

  @Override public String getName() { return name; }
  @Override public Integer getPrecision() { return precision; }
  @Override public Integer getScale() { return scale; }
  @Override public Object getNativeTypeMetadata() { return nativeMeta; }

  @Override
  public Type getType() {
    return tdsType.r2dbcType;
  }

  @Override
  public Class<?> getJavaType() {
    return tdsType.r2dbcType.getJavaType();
  }

  @Override
  public Nullability getNullability() {
    return Nullability.NULLABLE;
  }
}