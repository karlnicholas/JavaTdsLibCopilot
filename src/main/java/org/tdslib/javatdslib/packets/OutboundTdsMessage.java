package org.tdslib.javatdslib.packets;

import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.headers.AllHeaders;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Represents a logical outbound TDS request.
 * The payload is a Publisher to support reactive streaming of Large Objects (LOBs).
 */
public final class OutboundTdsMessage {

  private final PacketType packetType;
  private final byte statusFlags;
  private final Publisher<ByteBuffer> payload;

  public OutboundTdsMessage(PacketType packetType, byte statusFlags, Publisher<ByteBuffer> payload) {
    this.packetType = packetType;
    this.statusFlags = statusFlags;
    this.payload = payload;
  }

  public static OutboundTdsMessage createRequest(PacketType packetType, Publisher<ByteBuffer> payload) {
    return new OutboundTdsMessage(packetType, PacketStatus.EOM, payload);
  }

  public static OutboundTdsMessage createWithHeaders(
      PacketType packetType, AllHeaders headers, Publisher<ByteBuffer> payload) {

    byte[] headerBytes = headers != null ? headers.toBytes() : new byte[0];

    if (headerBytes.length == 0) {
      return createRequest(packetType, payload);
    }

    ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);
    Publisher<ByteBuffer> combined = Flux.concat(Mono.just(headerBuf), payload);

    return createRequest(packetType, combined);
  }

  public PacketType getPacketType() {
    return packetType;
  }

  public byte getStatusFlags() {
    return statusFlags;
  }

  public Publisher<ByteBuffer> getPayload() {
    return payload;
  }

  /**
   * TEMPORARY BRIDGE: Synchronously consumes the reactive stream into a single ByteBuffer.
   * To be removed once QueryPacketBuilder supports streaming.
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
        .subscribe(b -> captured[0] = b);

    if (captured[0] == null) {
      throw new IllegalStateException("Temporary bridge failed: Publisher did not execute synchronously.");
    }

    return captured[0];
  }
}