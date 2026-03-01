package org.tdslib.javatdslib.api;

import io.r2dbc.spi.OutParameterMetadata;
import io.r2dbc.spi.Type;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;

public class TdsOutParameterMetadata implements OutParameterMetadata {
  private final String name;
  private final TdsType tdsType;
  private final int scale;
  private final ReturnValueToken nativeToken;

  public TdsOutParameterMetadata(ReturnValueToken token) {
    this.nativeToken = token;
    this.name = token.getParamName();
    this.tdsType = token.getTypeInfo().getTdsType();
    this.scale = token.getTypeInfo().getScale();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Class<?> getJavaType() {
    return tdsType.r2dbcType.getJavaType();
  }

  @Override
  public Type getType() {
    return tdsType.r2dbcType;
  }

  // These specific methods resolve the compiler errors in TdsRowSegment
  public TdsType getTdsType() {
    return tdsType;
  }

  public Integer getScale() {
    return scale;
  }
}