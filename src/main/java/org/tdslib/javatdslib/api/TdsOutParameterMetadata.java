package org.tdslib.javatdslib.api;

import io.r2dbc.spi.OutParameterMetadata;
import io.r2dbc.spi.Type;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;

/**
 * Implementation of {@link OutParameterMetadata} for the TDS protocol.
 * This class provides metadata information about an output parameter returned from a stored procedure,
 * such as its name, type, and scale.
 */
public class TdsOutParameterMetadata implements OutParameterMetadata {
  private final String name;
  private final TdsType tdsType;
  private final int scale;
  private final ReturnValueToken nativeToken;

  /**
   * Constructs a new TdsOutParameterMetadata instance.
   *
   * @param token The return value token containing the output parameter details.
   */
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
