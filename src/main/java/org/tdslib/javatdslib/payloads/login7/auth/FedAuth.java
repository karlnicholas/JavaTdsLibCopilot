package org.tdslib.javatdslib.payloads.login7.auth;

import java.nio.ByteBuffer;

/**
 * Federated authentication feature extension base.
 */
public abstract class FedAuth {
  protected static final byte FeatureId = 0x02;
  protected static final byte LibrarySecurityToken = 0x02;
  protected static final byte LibraryADAL = 0x04;
  protected static final byte FedAuthEchoYes = 0x01;
  protected static final byte FedAuthEchoNo = 0x00;

  /**
   * Protected default constructor for subclasses.
   */
  protected FedAuth() {
  }

  /**
   * Build the federated authentication extension as a ByteBuffer.
   *
   * @return a ByteBuffer containing the extension payload (little-endian, ready to be appended)
   */
  public abstract ByteBuffer toByteBuffer();
}
