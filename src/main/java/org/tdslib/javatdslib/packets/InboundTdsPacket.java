package org.tdslib.javatdslib.packets;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

/**
 * Represents a single TDS packet received from the server.
 * The payload is a fully resolved, in-memory ByteBuffer.
 */
public final class InboundTdsPacket {

  private final PacketType packetType;
  private final byte statusFlags;
  private final int packetLength;
  private final short spid;
  private final short packetNumber;
  private final ByteBuffer payload;

  private final boolean isLastPacket;
  private final long receivedAt;
  private final String traceContext;

  public InboundTdsPacket(
      PacketType packetType,
      byte statusFlags,
      int packetLength,
      short spid,
      short packetNumber,
      ByteBuffer payload,
      long receivedAt,
      String traceContext) {

    this.packetType = packetType;
    this.statusFlags = statusFlags;
    this.packetLength = packetLength;
    this.spid = spid;
    this.packetNumber = packetNumber;

    if (payload != null) {
      this.payload = payload.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
    } else {
      this.payload = null;
    }

    this.isLastPacket = (statusFlags & PacketStatus.EOM) != 0;
    this.receivedAt = receivedAt;
    this.traceContext = traceContext;
  }

  public PacketType getPacketType() { return packetType; }
  public byte getStatusFlags() { return statusFlags; }
  public int getPacketLength() { return packetLength; }
  public short getSpid() { return spid; }
  public short getPacketNumber() { return packetNumber; }
  public ByteBuffer getPayload() { return payload; }
  public boolean isLastPacket() { return isLastPacket; }

  public boolean isResetConnection() {
    return (statusFlags & PacketStatus.RESET_CONNECTION) != 0;
  }

  public boolean isIgnore() {
    return (statusFlags & PacketStatus.IGNORE) != 0;
  }

  public boolean isFromServer() {
    return (statusFlags & 0x04) != 0;
  }

  public long getReceivedAt() {
    return receivedAt;
  }

  public Instant getReceivedInstant() {
    if (receivedAt <= 0) {
      return null;
    }
    long nowMillis = System.currentTimeMillis();
    long elapsedMillis = (System.nanoTime() - receivedAt) / 1_000_000L;
    return Instant.ofEpochMilli(nowMillis - elapsedMillis);
  }

  public String getTraceContext() {
    return traceContext;
  }

  @Override
  public String toString() {
    String fmt = "InboundTdsPacket{type=%s (0x%02X), packet=%d, length=%d, spid=%d,"
        + " last=%b, reset=%b, payloadBytes=%d}";
    return String.format(
        fmt,
        packetType != null ? packetType.name() : "UNKNOWN",
        packetType != null ? packetType.getValue() & 0xFF : 0,
        packetNumber,
        packetLength,
        spid,
        isLastPacket,
        isResetConnection(),
        payload != null ? payload.remaining() : 0
    );
  }
}