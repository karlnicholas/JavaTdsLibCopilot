package org.tdslib.javatdslib.tokens.featureextack;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * FEATURE_EXT_ACK token (0xAE) - acknowledges feature extensions (e.g., FedAuth).
 */
public final class FeatureExtAckToken extends Token {

  private final byte featureId;
  private final byte[] data; // Raw data bytes following the ID

  /**
   * Constructs a new FeatureExtAckToken.
   *
   * <p>Creates an immutable token instance with the provided token type,
   * feature identifier and associated raw data.</p>
   *
   * @param tokenType the raw token type byte as received from the stream
   * @param featureId the feature extension identifier
   * @param data      the raw feature data bytes; may be {@code null}
   */
  public FeatureExtAckToken(final byte tokenType, final byte featureId, final byte[] data) {
    super(TokenType.fromValue(tokenType));
    this.featureId = featureId;
    this.data = data != null ? data.clone() : new byte[0];
  }

  /**
   * Gets the feature extension ID (e.g., FED_AUTH = 0x02).
   */
  public byte getFeatureId() {
    return featureId;
  }

  /**
   * Gets the raw data bytes associated with this feature (may be empty).
   */
  public byte[] getData() {
    return data.clone();
  }

  @Override
  public String toString() {
    return String.format(
        "FeatureExtAckToken{featureId=0x%02X, dataLength=%d}",
        featureId & 0xFF, data.length
    );
  }
}
