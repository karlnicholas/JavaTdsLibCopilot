package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.models.FeatureExtAckToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

public class FeatureExtAckTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context) {
    if (tokenType != TokenType.FEATURE_EXT_ACK.getValue()) {
      throw new IllegalArgumentException("Expected FEATURE_EXT_ACK token (0xAE)");
    }

    // TDS uses Little Endian for all multi-byte integers
    payload.order(ByteOrder.LITTLE_ENDIAN);
    Map<Byte, byte[]> features = new HashMap<>();

    while (payload.hasRemaining()) {
      byte featureId = payload.get();
      if (featureId == (byte) 0xFF) { // 255 = Terminator
        break;
      }

      // Feature Data Length is a 4-byte integer
      int featureLen = payload.getInt();
      byte[] data = new byte[featureLen];
      if (featureLen > 0) {
        payload.get(data);
      }

      features.put(featureId, data);
    }

    // Return the immutable token with no side-effects
    return new FeatureExtAckToken(tokenType, features);
  }
}