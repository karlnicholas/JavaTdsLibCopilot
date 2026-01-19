package org.tdslib.javatdslib.packets;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Represents a Packet in the TDS protocol.
 *
 * <p>Provides helpers to access header fields and payload slices.
 */
public class Packet {

  // Constants
  public static final int HEADER_LENGTH = 8;
  public static final int DEFAULT_SPID = 0;
  public static final int DEFAULT_PACKET_ID = 1;
  public static final int DEFAULT_WINDOW = 0;

  // Offsets for absolute access
  private static final int OFFSET_TYPE = 0;
  private static final int OFFSET_STATUS = 1;
  private static final int OFFSET_LENGTH = 2;
  private static final int OFFSET_SPID = 4;
  private static final int OFFSET_PACKET_ID = 6;
  private static final int OFFSET_WINDOW = 7;

  private ByteBuffer buffer;

  // --- Absolute Accessors ---

  /**
   * Returns the packet type as a {@link PacketType}.
   *
   * @return the packet type
   */
  public PacketType getType() {
    int typeVal = Byte.toUnsignedInt(buffer.get(OFFSET_TYPE));
    try {
      return PacketType.valueOf(typeVal);
    } catch (IllegalArgumentException e) {
      throw e;
    }
  }

  /**
   * Returns the raw status byte as an unsigned integer.
   *
   * @return status flags
   */
  public int getStatus() {
    return Byte.toUnsignedInt(buffer.get(OFFSET_STATUS));
  }

  /**
   * Returns the SPID (server process id) as an unsigned integer.
   *
   * <p>Note: method name follows camelCase (`getSpid`) to satisfy style rules.
   *
   * @return SPID value
   */
  public int getSpid() {
    return Short.toUnsignedInt(buffer.getShort(OFFSET_SPID));
  }

  /**
   * Returns the packet sequence id.
   *
   * @return packet id (0-255)
   */
  public int getId() {
    return Byte.toUnsignedInt(buffer.get(OFFSET_PACKET_ID));
  }

  /**
   * Sets the packet id (low 8 bits used).
   *
   * @param id packet id (0-255)
   */
  public void setId(int id) {
    buffer.put(OFFSET_PACKET_ID, (byte) id);
  }

  /**
   * Returns the window byte.
   *
   * @return window value (0-255)
   */
  public int getWindow() {
    return Byte.toUnsignedInt(buffer.get(OFFSET_WINDOW));
  }

  /**
   * Returns the full packet length (header + data).
   *
   * @return packet length
   */
  public int getLength() {
    return Short.toUnsignedInt(buffer.getShort(OFFSET_LENGTH));
  }

  // --- Status Flags ---

  /**
   * Returns true when this packet has EOM (End Of TdsMessage).
   *
   * @return true if last packet in logical message
   */
  public boolean isLast() {
    return (getStatus() & PacketStatus.EOM) == PacketStatus.EOM;
  }

  /**
   * Sets or clears the EOM flag.
   *
   * @param last whether this is the last packet
   */
  public void setLast(boolean last) {
    int status = getStatus();
    if (last) {
      status |= PacketStatus.EOM;
    } else {
      status &= ~PacketStatus.EOM;
    }
    buffer.put(OFFSET_STATUS, (byte) status);
  }

  /**
   * Returns true when IGNORE flag is set.
   *
   * @return true if IGNORE flag set
   */
  public boolean isIgnore() {
    return (getStatus() & PacketStatus.IGNORE) == PacketStatus.IGNORE;
  }

  /**
   * Sets or clears the IGNORE flag.
   *
   * @param ignore whether to set IGNORE
   */
  public void setIgnore(boolean ignore) {
    int status = getStatus();
    if (ignore) {
      status |= PacketStatus.IGNORE;
    } else {
      status &= ~PacketStatus.IGNORE;
    }
    buffer.put(OFFSET_STATUS, (byte) status);
  }

  /**
   * Returns true when RESET_CONNECTION flag is set.
   *
   * @return true if RESET_CONNECTION flag set
   */
  public boolean isResetConnection() {
    return (getStatus() & PacketStatus.RESET_CONNECTION) == PacketStatus.RESET_CONNECTION;
  }

  /**
   * Sets or clears the RESET_CONNECTION flag.
   *
   * @param resetConnection whether to set RESET_CONNECTION
   */
  public void setResetConnection(boolean resetConnection) {
    int status = getStatus();
    if (resetConnection) {
      status |= PacketStatus.RESET_CONNECTION;
    } else {
      status &= ~PacketStatus.RESET_CONNECTION;
    }
    buffer.put(OFFSET_STATUS, (byte) status);
  }

  /**
   * Returns a slice of the data portion.
   * CRITICAL: Forces Little Endian order for the payload.
   *
   * @return read-only little-endian view of payload bytes
   */
  public ByteBuffer getData() {
    if (buffer.capacity() <= HEADER_LENGTH) {
      return ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);
    }
    // slice() resets to Big Endian. We must force Little Endian for TDS payloads.
    return buffer.slice(HEADER_LENGTH, buffer.capacity() - HEADER_LENGTH)
        .order(ByteOrder.LITTLE_ENDIAN)
        .asReadOnlyBuffer();
  }

  /**
   * Returns the full backing buffer (Header + Data).
   * Used when writing the packet to the network.
   *
   * @return duplicate of internal buffer positioned at start
   */
  public ByteBuffer getBuffer() {
    // Return a duplicate so the caller cannot mess up the position/limit
    // of the internal buffer logic.
    return buffer.duplicate().rewind();
  }

  // --- Constructors ---

  /**
   * Creates a new empty packet with the provided type and default header fields.
   *
   * @param type packet type
   */
  public Packet(PacketType type) {
    // Headers are always Big Endian
    this.buffer = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN);

    buffer.put(OFFSET_TYPE, (byte) type.getValue());
    buffer.put(OFFSET_STATUS, PacketStatus.NORMAL);
    buffer.putShort(OFFSET_SPID, (short) DEFAULT_SPID);
    buffer.put(OFFSET_PACKET_ID, (byte) DEFAULT_PACKET_ID);
    buffer.put(OFFSET_WINDOW, (byte) DEFAULT_WINDOW);
    updateLength();
  }

  /**
   * Wraps an existing buffer as a Packet. Validates header and byte order.
   *
   * @param buffer backing buffer (must be at least HEADER_LENGTH)
   * @throws IllegalArgumentException when buffer is null, too small, or has invalid type
   */
  public Packet(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "Buffer cannot be null");

    // Ensure we treat headers as Big Endian
    if (buffer.order() != ByteOrder.BIG_ENDIAN) {
      buffer.order(ByteOrder.BIG_ENDIAN);
    }

    if (buffer.capacity() < HEADER_LENGTH) {
      throw new IllegalArgumentException("Buffer length must be at least " + HEADER_LENGTH);
    }

    int typeValue = Byte.toUnsignedInt(buffer.get(OFFSET_TYPE));
    try {
      PacketType.valueOf(typeValue);
    } catch (IllegalArgumentException e) {
      String msg = String.format("Invalid packet type: 0x%02X", typeValue);
      throw new IllegalArgumentException(msg, e);
    }

    this.buffer = buffer;
  }

  /**
   * Update the header length field to match the buffer capacity.
   */
  private void updateLength() {
    buffer.putShort(OFFSET_LENGTH, (short) buffer.capacity());
  }

  /**
   * Sets the packet id using modulo 256 semantics.
   *
   * @param packetId desired packet id
   */
  public void setPacketId(int packetId) {
    setId(packetId % 256);
  }

  /**
   * Appends data to the packet payload. If data is empty or null this is a no-op.
   *
   * @param data source buffer whose remaining bytes will be appended
   */
  public void addData(ByteBuffer data) {
    if (data == null || !data.hasRemaining()) {
      return;
    }

    int newCapacity = this.buffer.capacity() + data.remaining();
    ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity).order(ByteOrder.BIG_ENDIAN);

    // Copy existing packet content without moving source position
    ByteBuffer src = this.buffer.duplicate();
    src.rewind();
    newBuffer.put(src);

    // Copy new data
    ByteBuffer dataSrc = data.duplicate();
    newBuffer.put(dataSrc);

    // CRITICAL FIX: Reset position to 0 so the buffer is ready for use/checks
    newBuffer.rewind();

    this.buffer = newBuffer;
    updateLength();
  }

  @Override
  public String toString() {
    String fmtPart1 = "Packet[Type=0x%02X(%s), Status=0x%02X, Length=%d, ";
    String fmtPart2 = "SPID=0x%04X, PacketId=%d, Window=0x%02X]";
    return String.format(
        fmtPart1 + fmtPart2,
        getType().getValue(),
        getType(),
        getStatus(),
        getLength(),
        getSpid(),
        getId(),
        getWindow()
    );
  }
}
