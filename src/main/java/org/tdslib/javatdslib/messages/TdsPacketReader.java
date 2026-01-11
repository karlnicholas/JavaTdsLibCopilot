package org.tdslib.javatdslib.messages;

import org.tdslib.javatdslib.transport.TcpTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Reads a complete TDS packet from the transport.
 */
public class TdsPacketReader {

  private final TcpTransport transport;

  /**
   * Constructs a new TdsPacketReader bound to the provided transport.
   *
   * @param transport underlying TCP transport used to read bytes
   */
  public TdsPacketReader(TcpTransport transport) {
    this.transport = transport;
  }

  /**
   * Reads exactly one complete TDS packet.
   * Returns the full packet as a ByteBuffer (header + payload).
   */
  public ByteBuffer readRawPacket() throws IOException {
    // First, read the fixed 8-byte header
    ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    transport.readFully(header);
    header.flip();

    byte packetType = header.get();
    byte status = header.get();
    int length = header.getShort() & 0xFFFF;  // unsigned short

    if (length < 8 || length > 32767) {
      throw new IOException("Invalid TDS packet length: " + length);
    }

    // Read the remaining payload
    ByteBuffer payloadBuffer = ByteBuffer.allocate(length - 8).order(ByteOrder.LITTLE_ENDIAN);
    transport.readFully(payloadBuffer);
    payloadBuffer.flip();

    // Combine header + payload into full packet
    ByteBuffer fullPacket = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
    fullPacket.put(header.array());
    fullPacket.put(payloadBuffer.array());
    fullPacket.flip();

    return fullPacket;
  }

  /**
   * Convenience method: read a full packet and return it as a Message.
   */
  public Message readPacket() throws IOException {
    ByteBuffer raw = readRawPacket();
    raw.mark();

    final byte type = raw.get();
    final byte status = raw.get();
    final int length = Short.toUnsignedInt(raw.getShort());
    final short spid = raw.getShort();
    final short packetId = raw.getShort();
    raw.get(); // window

    raw.reset();
    raw.position(8);
    ByteBuffer payload = raw.slice().limit(length - 8);

    return new Message(
        type,
        status,
        length,
        spid,
        packetId,
        payload,
        System.nanoTime(),
        null
    );
  }
}
