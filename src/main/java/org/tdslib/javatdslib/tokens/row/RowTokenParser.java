package org.tdslib.javatdslib.tokens.row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;

public class RowTokenParser implements TokenParser {

  private static final Logger log = LoggerFactory.getLogger(RowTokenParser.class);

  @Override
  public Token parse(final ByteBuffer payload, final byte tokenType,
                     final ConnectionContext context) {
    if (tokenType != (byte) 0xD1) {
      throw new IllegalArgumentException("Expected ROW (0xD1), got 0x" + Integer.toHexString(tokenType & 0xFF));
    }

    // Defer decoding to the stateful visitor which holds the ColMetaDataToken.
    // We pass the payload buffer down. The visitor MUST advance the buffer position.
    return new RawRowToken(tokenType, payload);
  }
}