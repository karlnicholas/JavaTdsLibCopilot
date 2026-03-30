package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Type;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.reactive.R2dbcTypeMapper;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;

/**
 * Implementation of {@link ColumnMetadata} for the TDS protocol.
 * This class provides metadata information about a column in a result set,
 * such as its name, precision, scale, and type.
 */
public class TdsColumnMetadata implements ColumnMetadata {
  private final String name;
  private final int precision;
  private final int scale;
  private final TdsType tdsType;
  private final Object nativeMeta;

  /**
   * Constructs a new TdsColumnMetadata instance.
   *
   * @param meta The underlying TDS column metadata token containing the column details.
   */
  public TdsColumnMetadata(ColumnMeta meta) {
    this.name = meta.getName();
    this.precision = meta.getMaxLength();
    this.scale = meta.getScale();
    this.tdsType = TdsType.valueOf(meta.getDataType());
    this.nativeMeta = meta;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Integer getPrecision() {
    return precision;
  }

  @Override
  public Integer getScale() {
    return scale;
  }

  @Override
  public Object getNativeTypeMetadata() {
    return nativeMeta;
  }

  @Override
  public Type getType() {
    // FIX: Use the new Mapper
    return R2dbcTypeMapper.toR2dbcType(tdsType);
  }

  @Override
  public Class<?> getJavaType() {
    // FIX: Use the new Mapper
    return R2dbcTypeMapper.toR2dbcType(tdsType).getJavaType();
  }

  @Override
  public Nullability getNullability() {
    return Nullability.NULLABLE;
  }
}
