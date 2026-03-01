package org.tdslib.javatdslib.tokens.models;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

import java.util.Collections;
import java.util.Map;

/**
 * FEATURE_EXT_ACK token (0xAE) - acknowledges feature extensions.
 */
public final class FeatureExtAckToken extends Token {

  private final Map<Byte, byte[]> features;

  /**
   * Constructs a new FeatureExtAckToken.
   *
   * @param tokenType the raw token type byte as received from the stream
   * @param features  map of acknowledged feature IDs to their data payloads
   */
  public FeatureExtAckToken(final byte tokenType, final Map<Byte, byte[]> features) {
    super(TokenType.fromValue(tokenType));
    this.features = features != null ? features : Collections.emptyMap();
  }

  /**
   * Gets the raw data bytes associated with a specific feature ID.
   */
  public byte[] getFeatureData(byte featureId) {
    return features.get(featureId);
  }

  /**
   * Helper method to easily check if UTF-8 was successfully negotiated.
   */
  public boolean isUtf8Negotiated() {
    byte[] data = features.get(FeatureId.UTF8_SUPPORT);
    // According to MS-TDS, data of 0x01 means enabled
    return data != null && data.length > 0 && data[0] == 0x01;
  }

  @Override
  public String toString() {
    return "FeatureExtAckToken{featuresCount=" + features.size() + "}";
  }
}