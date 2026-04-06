package org.tdslib.javatdslib.headers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Trace Activity Header (type 0x0003).
 * Introduced in TDS 7.4 for tracking client activity in SQL Server Extended Events.
 */
public class TraceActivityHeader extends TdsHeader {

  private final UUID activityId;
  private final int activitySequence;

  public TraceActivityHeader(UUID activityId, int activitySequence) {
    super((short) 0x0003);
    if (activityId == null) {
      throw new IllegalArgumentException("ActivityId cannot be null");
    }
    this.activityId = activityId;
    this.activitySequence = activitySequence;
  }

  @Override
  public int getLength() {
    // HeaderLength (4) + HeaderType (2) + GUID (16) + ActivitySequence (4)
    return 26;
  }

  @Override
  public void write(ByteBuffer buffer) {
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(getLength());              // 4 bytes
    buffer.putShort(type);                   // 2 bytes

    writeMicrosoftGuid(buffer, activityId);  // 16 bytes
    buffer.putInt(activitySequence);         // 4 bytes
  }

  /**
   * Microsoft GUIDs use a mixed-endian layout.
   * Data1 (4 bytes), Data2 (2 bytes), and Data3 (2 bytes) are Little-Endian.
   * Data4 (8 bytes) is Big-Endian.
   */
  private void writeMicrosoftGuid(ByteBuffer buffer, UUID uuid) {
    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();

    // Data1: 32-bit (Little Endian)
    buffer.putInt((int) (msb >>> 32));
    // Data2: 16-bit (Little Endian)
    buffer.putShort((short) (msb >>> 16));
    // Data3: 16-bit (Little Endian)
    buffer.putShort((short) msb);

    // Data4: 64-bit (Big Endian sequence of bytes)
    // Since the ByteBuffer is currently LITTLE_ENDIAN, we must write
    // these remaining 8 bytes one by one to preserve network byte order.
    for (int i = 7; i >= 0; i--) {
      buffer.put((byte) (lsb >>> (8 * i)));
    }
  }
}