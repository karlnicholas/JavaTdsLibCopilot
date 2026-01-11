package org.tdslib.javatdslib.headers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * ALL_HEADERS wrapper: multiple headers + TotalLength (DWORD, including itself).
 * For simple batch, usually just one TransactionDescriptor header.
 */
public class AllHeaders {

  private final org.tdslib.javatdslib.tds.headers.TdsHeader[] headers;

  public AllHeaders(org.tdslib.javatdslib.tds.headers.TdsHeader... headers) {
    this.headers = headers;
  }

  /**
   * Builds the ALL_HEADERS bytes (ready to prepend before SQL text).
   */
  public byte[] toBytes() {
    int dataLength = 0;
    for (org.tdslib.javatdslib.tds.headers.TdsHeader h : headers) {
      dataLength += h.getLength();
    }

    int totalLength = 4 + dataLength;  // TotalLength DWORD + headers

    ByteBuffer buffer = ByteBuffer.allocate(totalLength);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.putInt(totalLength);  // TotalLength (includes itself)

    for (org.tdslib.javatdslib.tds.headers.TdsHeader h : headers) {
      h.write(buffer);
    }

    return buffer.array();
  }

  /**
   * Convenience factory for the most common case: auto-commit simple query.
   */
  public static AllHeaders forAutoCommit(int outstandingRequestCount) {
    return new AllHeaders(
        new TransactionDescriptorHeader(0L, outstandingRequestCount)  // or 1L, 1 – both work
    );
  }
}