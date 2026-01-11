package org.tdslib.javatdslib.headers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Transaction Descriptor Header (type 0x0002) – required/strongly recommended for SQLBatch.
 * Used for MARS and transaction context.
 */
public class TransactionDescriptorHeader extends org.tdslib.javatdslib.tds.headers.TdsHeader {

  private final long transactionDescriptor;      // ULONGLONG (8 bytes)
  private final int outstandingRequestCount;     // DWORD (4 bytes)

  /**
   * Constructor for auto-commit mode (typical for simple queries).
   * - transactionDescriptor = 0 or 1 (server ignores in auto-commit)
   * - outstandingRequestCount = 0 or 1
   */
  public TransactionDescriptorHeader(long transactionDescriptor, int outstandingRequestCount) {
    super((short) 0x0002);
    this.transactionDescriptor = transactionDescriptor;
    this.outstandingRequestCount = outstandingRequestCount;
  }

  @Override
  public int getLength() {
    return 4 + 2 + 8 + 4;  // HeaderLength(4) + Type(2) + TransDesc(8) + Outstanding(4)
  }

  @Override
  public void write(ByteBuffer buffer) {
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(getLength());                    // HeaderLength
    buffer.putShort(type);                         // HeaderType
    buffer.putLong(transactionDescriptor);         // ULONGLONG
    buffer.putInt(outstandingRequestCount);        // DWORD
  }
}

