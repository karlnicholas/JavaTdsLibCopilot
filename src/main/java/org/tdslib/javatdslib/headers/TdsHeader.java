package org.tdslib.javatdslib.headers;

import java.nio.ByteBuffer;

/**
 * Base class for individual TDS headers in ALL_HEADERS.
 * Each header has: HeaderLength (DWORD), HeaderType (USHORT), HeaderData (variable bytes).
 */
public abstract class TdsHeader {

  protected final short type;  // e.g., 0x0002 for TransactionDescriptor

  /**
   * Construct a TdsHeader with the given header type code.
   *
   * @param type header type identifier
   */
  public TdsHeader(short type) {
    this.type = type;
  }

  /**
   * Returns the total length of this header (including HeaderLength + HeaderType + data).
   */
  public abstract int getLength();

  /**
   * Writes the header to the buffer (little-endian).
   */
  public abstract void write(ByteBuffer buffer);
}
