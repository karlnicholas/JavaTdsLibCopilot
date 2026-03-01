package org.tdslib.javatdslib.rpc;

import java.nio.charset.Charset;

/**
 * Carries connection-specific encoding state needed by codecs.
 *
 * @param varcharCharset the charset to use for VARCHAR encoding
 * @param collationBytes the 5-byte collation signature
 */
public record RpcEncodingContext(
    Charset varcharCharset,
    byte[] collationBytes
) {
  /**
   * Compact constructor to ensure valid collation bytes.
   *
   * @param varcharCharset the charset to use for VARCHAR encoding
   * @param collationBytes the 5-byte collation signature
   */
  public RpcEncodingContext {
    // Fallback to CP1252 (Sort ID 52) if missing
    if (collationBytes == null || collationBytes.length < 5) {
      collationBytes = new byte[]{0x09, 0x04, (byte) 0xD0, 0x00, 0x34};
    }
  }
}