package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

public class TdsColumnMetadataImpl implements ColumnMetadata {
  private final ColumnMeta meta;

  TdsColumnMetadataImpl(ColumnMeta meta) {
    this.meta = meta;
  }

  @Override public String getName() { return meta.getName(); }
  @Override public Integer getPrecision() { return meta.getMaxLength(); }
  @Override public Integer getScale() { return (int) meta.getScale(); }
  @Override public Object getNativeTypeMetadata() { return meta; }

  /**
   * Required by io.r2dbc.spi.ReadableMetadata.
   * Returns the R2DBC Type using R2dbcType constants.
   */
  @Override
  public Type getType() {
    byte tdsType = meta.getDataType();
    return TdsType.valueOf(tdsType).r2dbcType;
  }

  /**
   * Required by R2DBC SPI versions 0.9+.
   * Returns the Java Class representation.
   */
  @Override
  public Class<?> getJavaType() {
    byte tdsType = meta.getDataType();
    return TdsType.valueOf(tdsType).r2dbcType.getJavaType();
  }

  @Override
  public Nullability getNullability() {
    // Bit 0x01 in TDS Status/Flags is Nullable
    return (meta.getFlags() & 0x01) != 0 ? Nullability.NULLABLE : Nullability.NON_NULL;
  }
}