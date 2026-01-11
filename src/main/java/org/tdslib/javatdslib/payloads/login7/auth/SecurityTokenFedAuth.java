package org.tdslib.javatdslib.payloads.login7.auth;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Federated authentication payload that carries a security token.
 */
public final class SecurityTokenFedAuth extends FedAuth {

  private final String token;
  private final boolean echo;

  /**
   * Create a SecurityTokenFedAuth.
   *
   * @param token the security token (must not be null or empty)
   * @param echo  whether the server should echo the challenge
   */
  public SecurityTokenFedAuth(final String token, final boolean echo) {
    if (token == null || token.isEmpty()) {
      throw new IllegalArgumentException("token");
    }
    this.token = token;
    this.echo = echo;
  }

  /**
   * Build the federated authentication extension as a ByteBuffer.
   *
   * @return ByteBuffer containing the security token extension (little-endian, flipped)
   */
  @Override
  public ByteBuffer toByteBuffer() {
    byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_16LE);
    ByteBuffer tokenBuffer = ByteBuffer.wrap(tokenBytes);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.put(FeatureId);
    buffer.putInt(tokenBuffer.remaining() + 4 + 1);
    byte options = (byte) (LibrarySecurityToken | (echo ? FedAuthEchoYes : FedAuthEchoNo));
    buffer.put(options);
    buffer.putInt(tokenBuffer.remaining());
    buffer.flip();

    ByteBuffer out = ByteBuffer.allocate(buffer.remaining() + tokenBuffer.remaining());
    out.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    out.put(buffer);
    out.put(tokenBuffer);
    out.flip();
    return out;
  }
}
