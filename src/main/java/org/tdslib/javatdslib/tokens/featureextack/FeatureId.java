package org.tdslib.javatdslib.tokens.featureextack;

/**
 * Known Feature Extension IDs for TDS protocol FEATURE_EXT_ACK token.
 */
public final class FeatureId {

  public static final byte FED_AUTH = 0x02;
  public static final byte UTF8_SUPPORT = 0x0A; // NEW: 10
  // Add other feature IDs as needed (e.g., 0x04 = Column Encryption, etc.)

  private FeatureId() {
  } // Utility class - no instances
}