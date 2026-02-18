package org.tdslib.javatdslib;

import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.OutParameterMetadata;
import io.r2dbc.spi.Type;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueToken;

public class TdsOutParameterMetadata implements OutParameterMetadata {
  private final String name;
  private final int precision;
  private final int scale;
  private final TdsType tdsType;
  private final Object nativeMeta;

  // Constructor for ReturnValueToken (Output Parameters)
  public TdsOutParameterMetadata(ReturnValueToken token) {
    this.name = token.getParamName();
    this.precision = 0;
    this.scale = 0;
    this.tdsType = token.getTypeInfo().getTdsType();
    this.nativeMeta = token;
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