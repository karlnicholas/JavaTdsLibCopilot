package org.tdslib.javatdslib.query.rpc;

import java.nio.charset.Charset;

/**
 * Carries connection-specific encoding state needed by codecs.
 */
public record RpcEncodingContext(
    Charset varcharCharset,
    byte[] collationBytes
) {
  public RpcEncodingContext {
    // Fallback to CP1252 (Sort ID 52) if missing
    if (collationBytes == null || collationBytes.length < 5) {
      collationBytes = new byte[]{0x09, 0x04, (byte) 0xD0, 0x00, 0x34};
    }
  }
}