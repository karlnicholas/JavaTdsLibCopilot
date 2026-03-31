package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.codec.DecoderRegistry;
import org.tdslib.javatdslib.protocol.CollationUtils;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.charset.Charset;
import java.util.List;

public class TdsOutSegment implements Result.OutSegment, OutParameters {
  private static final Logger logger = LoggerFactory.getLogger(TdsOutSegment.class);

  private final List<ReturnValueToken> parameters;
  private final ConnectionContext context;

  public TdsOutSegment(List<ReturnValueToken> parameters, ConnectionContext context) {
    this.parameters = parameters;
    this.context = context;
  }

  @Override
  public OutParameters outParameters() {
    return this; // Implement Result.OutSegment natively
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    if (index < 0 || index >= parameters.size()) {
      throw new IllegalArgumentException("Invalid OutParameter Index: " + index);
    }
    return decodeValue(parameters.get(index), type);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    for (ReturnValueToken param : parameters) {
      // Handle SQL Server's '@' prefix gracefully
      if (param.getParamName().equalsIgnoreCase(name) ||
          param.getParamName().equalsIgnoreCase("@" + name)) {
        return decodeValue(param, type);
      }
    }
    throw new IllegalArgumentException("OutParameter not found: " + name);
  }

  private <T> T decodeValue(ReturnValueToken token, Class<T> type) {
    if (token.getValue() == null) {
      return null;
    }

    TdsType tdsType = token.getTypeInfo().getTdsType();

    // Leverage the exact same charset resolution logic you built for TdsRow
    Charset charset;
    if (tdsType == TdsType.NVARCHAR || tdsType == TdsType.NCHAR || tdsType == TdsType.NTEXT) {
      charset = java.nio.charset.StandardCharsets.UTF_16LE;
    } else {
      byte[] collation = token.getTypeInfo() != null ? token.getTypeInfo().getCollation() : null;
      charset = collation != null
          ? CollationUtils.getCharsetFromCollation(collation).orElse(context.getVarcharCharset())
          : context.getVarcharCharset();
    }

    logger.trace("[TdsOutSegment] Decoding param '{}': Type={}, Target={}, Bytes={}",
        token.getParamName(), tdsType, type.getSimpleName(), token.getValue().length);

    return DecoderRegistry.DEFAULT.decode(token.getValue(), tdsType, type, 0, charset);
  }

  @Override
  public OutParametersMetadata getMetadata() {
    // Basic implementation: Can be expanded similarly to TdsRowMetadata later
    return null;
  }
}