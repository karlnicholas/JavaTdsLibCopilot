package org.tdslib.javatdslib.packets;

import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.headers.AllHeaders;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.Objects;

/**
 * Represents a single TDS (Tabular Data Stream) packet/message.
 * Immutable once created.
 */
public final class TdsMessage {

  private final PacketType packetType;
  private final byte statusFlags;
  private final int packetLength;
  private final short spid;
  private final short packetNumber;
//  private final ByteBuffer payload;
// In TdsMessage.java

  private final Publisher<ByteBuffer> payload;

  private final boolean isLastPacket;

  private final long receivedAt;
  private final String traceContext;

  /**
   * Full constructor for incoming packets.
   *
   * @param packetType   TDS packet type byte
   * @param statusFlags  Status flags byte
   * @param packetLength Total packet length (header + payload)
   * @param spid         Server process id (SPID)
   * @param packetNumber Packet sequence number
   * @param payload      Payload buffer (will be wrapped as read-only, little-endian)
   * @param receivedAt   Timestamp marker (nanotime-based) when the packet was received, or 0
   * @param traceContext Optional trace context string
   */
  public TdsMessage(
      PacketType packetType,
      byte statusFlags,
      int packetLength,
      short spid,
      short packetNumber,
      Publisher<ByteBuffer> payload,
      long receivedAt,
      String traceContext) {

    this.packetType = packetType;
    this.statusFlags = statusFlags;
    this.packetLength = packetLength;
    this.spid = spid;
    this.packetNumber = packetNumber;

//    this.payload = Objects.requireNonNull(payload, "Payload cannot be null")
//        .asReadOnlyBuffer()
//        .order(ByteOrder.LITTLE_ENDIAN);
    this.payload = payload;

    this.isLastPacket = (statusFlags & 0x01) != 0;

    this.receivedAt = receivedAt;
    this.traceContext = traceContext;
  }

  /**
   * TEMPORARY BRIDGE: Blocks the reactive stream and consolidates it into a single ByteBuffer.
   * To be removed once PacketEncoder supports streaming.
   */
  /**
   * TEMPORARY BRIDGE: Synchronously consumes the reactive stream into a single ByteBuffer.
   * To be removed once PacketEncoder supports streaming.
   */
  public ByteBuffer getPayloadSync() {
    final ByteBuffer[] captured = new ByteBuffer[1];

    Flux.from(this.payload)
        .reduce((b1, b2) -> {
          ByteBuffer combined = ByteBuffer.allocate(b1.remaining() + b2.remaining())
              .order(ByteOrder.LITTLE_ENDIAN);
          combined.put(b1).put(b2);
          return combined.flip();
        })
        .defaultIfEmpty(ByteBuffer.allocate(0))
        .subscribe(b -> captured[0] = b); // Executes synchronously for Mono.just()

    // Safety check in case an actual async Publisher sneaks in before we're ready
    if (captured[0] == null) {
      throw new IllegalStateException(
          "Temporary bridge failed: Publisher did not execute synchronously. " +
              "Ensure only Mono.just() or synchronous streams are passed to TdsMessage."
      );
    }

    return captured[0];
  }

  // ── Convenience factory methods for outgoing requests ───────────────────

  /**
   * Creates a simple outgoing request message with EOM status.
   * Use this for most requests (PreLogin, Login7, SQL Batch, RPC, etc.).
   *
   * @param packetType TDS packet type (e.g. 0x12 for PreLogin, 0x10 for Login7)
   * @param payload    The payload buffer (positioned at 0)
   * @return New TdsMessage instance ready to send
   */
//  public static TdsMessage createRequest(PacketType packetType, ByteBuffer payload) {
//    return new TdsMessage(
//        packetType,
//        (byte) 0x01,                  // EOM = true (single packet request)
//        payload.capacity() + 8,
//        (short) 0,                    // Client sends SPID 0
//        (short) 1,                    // First packet
//        payload,
//        0L,                           // No receive time for outgoing
//        null                          // No trace for outgoing (can be set later)
//    );
//  }

  public static TdsMessage createRequest(PacketType packetType, Publisher<ByteBuffer> payload) {
    return new TdsMessage(
        packetType, (byte) 0x01, -1, (short) 0, (short) 1, payload, 0L, null
    );
  }

  /**
   * Helper factory to create a request message that includes an ALL_HEADERS block.
   *
   * @param packetType The TDS packet type (e.g., PacketType.TRANSACTION_MANAGER.getValue())
   * @param headers    The AllHeaders block to prepend (can be null if none needed)
   * @param payload    The actual request payload
   * @return A constructed TdsMessage
   */
  public static TdsMessage createWithHeaders(
      PacketType packetType, AllHeaders headers, Publisher<ByteBuffer> payload) {

    byte[] headerBytes = headers != null ? headers.toBytes() : new byte[0];

    if (headerBytes.length == 0) {
      return createRequest(packetType, payload);
    }

    ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);

    // Reactively prepend the headers to whatever stream the payload represents
    Publisher<ByteBuffer> combined = Flux.concat(Mono.just(headerBuf), payload);

    return createRequest(packetType, combined);
  }

//  public static TdsMessage createWithHeaders(
//      PacketType packetType, AllHeaders headers, ByteBuffer payload) {
//    byte[] headerBytes = headers != null ? headers.toBytes() : new byte[0];
//
//    ByteBuffer fullPayload = ByteBuffer.allocate(headerBytes.length + payload.remaining());
//    if (headerBytes.length > 0) {
//      fullPayload.put(headerBytes);
//    }
//    fullPayload.put(payload);
//    fullPayload.flip();
//
//    return createRequest(packetType, fullPayload);
//  }

  // ── Getters ─────────────────────────────────────────────────────────────

  public PacketType getPacketType() {
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

//  public ByteBuffer getPayload() {
//    return payload.asReadOnlyBuffer();
//  }
public Publisher<ByteBuffer> getPayload() {
  return payload;
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

  /**
   * Returns the approximate Instant when this message was received.
   * The value is derived from the stored nanotime marker and system clock.
   *
   * @return {@link Instant} representing the receive time, or {@code null}
   *         when no receive timestamp is available
   */
  public Instant getReceivedInstant() {
    if (receivedAt <= 0) {
      return null;
    }
    long nowMillis = System.currentTimeMillis();
    long elapsedMillis = (System.nanoTime() - receivedAt) / 1_000_000L;
    return Instant.ofEpochMilli(nowMillis - elapsedMillis);
  }

  /**
   * Optional tracing context associated with this message, or null.
   *
   * @return trace context string or null
   */
  public String getTraceContext() {
    return traceContext;
  }

  // ── Utility ─────────────────────────────────────────────────────────────

  @Override
  public String toString() {
    // FIX: Removed the byte count metric since we cannot synchronously read a Publisher
    String fmt = "TdsMessage{type=%s (0x%02X), packet=%d, length=%d, spid=%d,"
        + " last=%b, reset=%b, payload=STREAM}";
    return String.format(
        fmt,
        packetType.getName(),
        packetType.getValue() & 0xFF,
        packetNumber,
        packetLength,
        spid,
        isLastPacket,
        isResetConnection()
    );
  }
}
