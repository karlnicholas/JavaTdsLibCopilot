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

/**
 * Parser for the FEATUREEXTACK token (0xAE). This token is sent by the server to acknowledge and
 * negotiate optional features requested by the client during the login process.
 */
public class FeatureExtAckTokenParser implements TokenParser {

  @Override
  public Token parse(
      final ByteBuffer payload, final byte tokenType, final ConnectionContext context) {
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

    return new FeatureExtAckToken(tokenType, features);
  }

  @Override
  public boolean canParse(ByteBuffer peekBuffer, ConnectionContext context) {
    peekBuffer.order(ByteOrder.LITTLE_ENDIAN);

    while (true) {
      // 1. Check if we have enough bytes to read the featureId (1 byte)
      if (peekBuffer.remaining() < 1) return false;

      byte featureId = peekBuffer.get();
      if (featureId == (byte) 0xFF) return true; // Terminator reached successfully

      // 2. Check if we have enough bytes to read the featureLen (4 bytes)
      if (peekBuffer.remaining() < 4) return false;
      int featureLen = peekBuffer.getInt();

      // 3. Check if we have enough bytes to skip the feature data
      if (peekBuffer.remaining() < featureLen) return false;

      // Skip past this feature's data to check the next one
      peekBuffer.position(peekBuffer.position() + featureLen);
    }
  }
}