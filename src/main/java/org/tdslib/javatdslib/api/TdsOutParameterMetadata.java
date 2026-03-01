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
    // FIX: Use the new Mapper
    return R2dbcTypeMapper.toR2dbcType(tdsType).getJavaType();
  }

  @Override
  public Type getType() {
    // FIX: Use the new Mapper
    return R2dbcTypeMapper.toR2dbcType(tdsType);
  }

  public TdsType getTdsType() {
    return tdsType;
  }

  public Integer getScale() {
    return scale;
  }
}