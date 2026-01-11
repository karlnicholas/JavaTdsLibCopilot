package org.tdslib.javatdslib.payloads.login7.auth;

/**
 * ADAL authentication workflow identifiers.
 */
public enum AdalWorkflow {
  UserPass((byte) 0x01),
  Integrated((byte) 0x02);

  private final byte value;

  AdalWorkflow(final byte v) {
    this.value = v;
  }

  public byte getValue() {
    return value;
  }
}
