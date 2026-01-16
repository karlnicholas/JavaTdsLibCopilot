package org.tdslib.javatdslib.tokens.info;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for INFO token (0xAB).
 *
 * <p>Eagerly decodes the full informational message details.</p>
 */
public class InfoTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context,
                     final QueryContext queryContext) {
    if (tokenType != TokenType.INFO.getValue()) {
      final String hex = Integer.toHexString(tokenType & 0xFF);
      throw new IllegalArgumentException("Expected INFO (0xAB), got 0x" + hex);
    }

    // Total length of the rest (bytes) — optional skip/validate
    Short.toUnsignedInt(payload.getShort());

    final long number = Integer.toUnsignedLong(payload.getInt());
    final byte state = payload.get();
    final byte severity = payload.get();

    // Message text: USHORT char count → bytes = count * 2
    final int msgCharLen = Short.toUnsignedInt(payload.getShort());
    String message = "";
    if (msgCharLen > 0) {
      final byte[] msgBytes = new byte[msgCharLen * 2];
      payload.get(msgBytes);
      message = new String(msgBytes, StandardCharsets.UTF_16LE).trim();
    }

    // Server name: BYTE char count → bytes = count * 2
    final int serverCharLen = payload.get() & 0xFF;
    String serverName = "";
    if (serverCharLen > 0) {
      final byte[] serverBytes = new byte[serverCharLen * 2];
      payload.get(serverBytes);
      serverName = new String(serverBytes, StandardCharsets.UTF_16LE).trim();
    }

    // Proc name: BYTE char count → bytes = count * 2
    final int procCharLen = payload.get() & 0xFF;
    String procName = "";
    if (procCharLen > 0) {
      final byte[] procBytes = new byte[procCharLen * 2];
      payload.get(procBytes);
      procName = new String(procBytes, StandardCharsets.UTF_16LE).trim();
    }

    // Line number: 2 or 4 bytes depending on TDS version
    final long lineNumber;
    if (context != null
        && context.getTdsVersion().ordinal() < TdsVersion.V7_2.ordinal()) {
      lineNumber = Short.toUnsignedInt(payload.getShort());
    } else {
      lineNumber = Integer.toUnsignedLong(payload.getInt());
    }

    return new InfoToken(
        tokenType,
        number,
        state,
        severity,
        message,
        serverName,
        procName,
        lineNumber
    );
  }

  private static String readUsVarChar(final ByteBuffer buf) {
    final int byteLen = Short.toUnsignedInt(buf.getShort());
    if (byteLen == 0xFFFF || byteLen == 0) {
      return "";
    }
    final byte[] bytes = new byte[byteLen];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_16LE).trim();
  }

  private static String readBvarChar(final ByteBuffer buf) {
    final int len = Byte.toUnsignedInt(buf.get());
    if (len == 0) {
      return "";
    }
    final byte[] bytes = new byte[len];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.US_ASCII).trim();
  }
}
