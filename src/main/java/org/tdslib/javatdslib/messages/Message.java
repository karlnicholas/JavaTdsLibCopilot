// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.Objects;

/**
 * Represents a single TDS (Tabular Data Stream) packet/message.
 * Immutable once created.
 */
public final class Message {

  private final byte packetType;
  private final byte statusFlags;
  private final int packetLength;
  private final short spid;
  private final short packetNumber;
  private final ByteBuffer payload;

  private final boolean isLastPacket;

  private final long receivedAt;
  private final String traceContext;

  // ── Full constructor (for incoming packets) ─────────────────────────────

  public Message(
      byte packetType,
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

    this.payload = Objects.requireNonNull(payload, "Payload cannot be null")
        .asReadOnlyBuffer()
        .order(ByteOrder.LITTLE_ENDIAN);

    this.isLastPacket = (statusFlags & 0x01) != 0;

    this.receivedAt = receivedAt;
    this.traceContext = traceContext;
  }

  // ── Convenience factory methods for outgoing requests ───────────────────

  /**
   * Creates a simple outgoing request message with EOM status.
   * Use this for most requests (PreLogin, Login7, SQL Batch, RPC, etc.).
   *
   * @param packetType TDS packet type (e.g. 0x12 for PreLogin, 0x10 for Login7)
   * @param payload    The payload buffer (positioned at 0)
   * @return New Message instance ready to send
   */
  public static Message createRequest(byte packetType, ByteBuffer payload) {
    return new Message(
        packetType,
        (byte) 0x01,           // EOM = true (single packet request)
        payload.capacity() + 8,
        (short) 0,             // Client sends SPID 0
        (short) 1,             // First packet
        payload,
        0L,                    // No receive time for outgoing
        null                   // No trace for outgoing (can be set later)
    );
  }

  /**
   * Creates a multi-packet request (for very large payloads).
   * The caller must manage packet number and status flags across packets.
   * Usually used via TdsPacketWriter, not directly.
   */
  public static Message createMultiPacketPart(byte packetType, byte statusFlags, short packetNumber, ByteBuffer payload) {
    return new Message(
        packetType,
        statusFlags,
        payload.capacity() + 8,
        (short) 0,
        packetNumber,
        payload,
        0L,
        null
    );
  }

  // ── Getters ─────────────────────────────────────────────────────────────

  public byte getPacketType() {
    return packetType;
  }

  public byte getStatusFlags() {
    return statusFlags;
  }

  public int getPacketLength() {
    return packetLength;
  }

  public short getSpid() {
    return spid;
  }

  public short getPacketNumber() {
    return packetNumber;
  }

  public ByteBuffer getPayload() {
    return payload.asReadOnlyBuffer();
  }

  public boolean isLastPacket() {
    return isLastPacket;
  }

  public boolean isResetConnection() {
    return (statusFlags & 0x08) != 0;
  }

  public boolean isFromServer() {
    return (statusFlags & 0x04) != 0;
  }

  public long getReceivedAt() {
    return receivedAt;
  }

  public Instant getReceivedInstant() {
    return receivedAt > 0 ? Instant.ofEpochMilli(System.currentTimeMillis() - (System.nanoTime() - receivedAt) / 1_000_000L) : null;
  }

  public String getTraceContext() {
    return traceContext;
  }

  // ── Utility ─────────────────────────────────────────────────────────────

  public String getPacketTypeName() {
    return switch (packetType) {
      case 0x01 -> "SQL Batch";
      case 0x03 -> "Login";
      case 0x04 -> "Tabular Result";
      case 0x06 -> "Attention";
      case 0x07 -> "RPC";
      case 0x0F -> "Bulk Load";
      case 0x10 -> "Login7";
      case 0x12 -> "PreLogin";
      default -> String.format("Unknown (0x%02X)", packetType & 0xFF);
    };
  }

  @Override
  public String toString() {
    return String.format(
        "Message{type=%s (0x%02X), packet=%d, length=%d, spid=%d, last=%b, reset=%b, payload=%d bytes}",
        getPacketTypeName(), packetType & 0xFF,
        packetNumber, packetLength, spid,
        isLastPacket, isResetConnection(), payload.remaining()
    );
  }
}