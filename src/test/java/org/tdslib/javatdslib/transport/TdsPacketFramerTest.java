package org.tdslib.javatdslib.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TdsPacketFramerTest {

  private TdsStreamHandler mockHandler;
  private TdsPacketFramer decoder;

  @BeforeEach
  void setUp() {
    mockHandler = mock(TdsStreamHandler.class);

    // The doAnswer buffer manipulation is no longer strictly required because
    // the new Framer ignores the downstream slice position.
    // Simple mocking is enough now.
    decoder = new TdsPacketFramer(mockHandler);
  }

  private ByteBuffer addFrame(ByteBuffer packet) {
    packet.put((byte) 0x01).put((byte) 0x01).putShort((short) 12) // Type, Status, Length (12)
        .putShort((short) 0).put((byte) 0).put((byte) 0);       // SPID, PID, Window
    return packet;
  }

  private ByteBuffer addPayload(ByteBuffer packet) {
    packet.put("ABCD".getBytes());
    return packet;
  }

  @Test
  @DisplayName("Should strip 8-byte header and forward payload")
  void testBasicHeaderStripping() {
    ByteBuffer packet = ByteBuffer.allocate(12).order(ByteOrder.BIG_ENDIAN);
    addFrame(packet);
    addPayload(packet);
    packet.flip();

    decoder.decode(packet);

    // Verify: The decoder should have processed all 12 bytes
    assertEquals(12, packet.position());
    verify(mockHandler).onPayloadAvailable(any(), eq(true));
  }

  @Test
  @DisplayName("Should handle header arriving in two separate TCP segments")
  void testFragmentedHeaderWithRealBufferLifecycle() {
    ByteBuffer readBuffer = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);

    // STAGE 1: Only 4 bytes arrive (header is incomplete)
    readBuffer.put((byte) 0x01).put((byte) 0x01).putShort((short) 12);
    readBuffer.flip();
    decoder.decode(readBuffer);

    assertEquals(0, readBuffer.position()); // No progress because header incomplete
    verify(mockHandler, never()).onPayloadAvailable(any(), anyBoolean());

    // NIO DANCE: Compact and add the rest
    readBuffer.compact();
    readBuffer.putShort((short) 50).put((byte) 1).put((byte) 0); // Rest of header
    readBuffer.put("ABCD".getBytes());                           // FIXED: Added missing payload
    readBuffer.flip();

    decoder.decode(readBuffer);

    // Verify: Now the full 12 bytes are consumed
    assertEquals(12, readBuffer.position());
    verify(mockHandler).onPayloadAvailable(any(), eq(true));
  }

  @Test
  @DisplayName("Should process a complete frame and then wait on a partial second frame")
  void testCompleteAndThenPartialFrame() {
    ByteBuffer readBuffer = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
    addFrame(readBuffer); // 8 bytes
    addPayload(readBuffer); // 4 bytes
    addFrame(readBuffer); // 8 bytes (Next header)

    readBuffer.flip();
    decoder.decode(readBuffer);

    // FIXED: The first packet is 12 bytes. It consumed 12, leaving 8 unread.
    assertEquals(12, readBuffer.position());
    verify(mockHandler, times(1)).onPayloadAvailable(any(), eq(true));
  }

  @Test
  @DisplayName("Should process a partial frame followed by the rest of the second frame")
  void testReassemblePartialSecondFrame() {
    ByteBuffer readBuffer = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
    addFrame(readBuffer); // 8 bytes (Packet 1)
    addPayload(readBuffer); // 4 bytes (Packet 1)
    addFrame(readBuffer); // 8 bytes (Packet 2 Header)

    readBuffer.flip();
    decoder.decode(readBuffer);

    // VERIFY: The first packet is fully consumed (12 bytes)
    assertEquals(12, readBuffer.position());
    verify(mockHandler, times(1)).onPayloadAvailable(any(), eq(true));

    // 2. STAGE 2: Provide the missing payload for the second packet
    readBuffer.compact(); // Moves the 8 unread header bytes to index 0. Position becomes 8.
    readBuffer.put("P2--".getBytes()); // Adds 4 bytes. Position becomes 12.
    readBuffer.flip(); // Limit becomes 12, Position becomes 0.

    decoder.decode(readBuffer);

    // VERIFY: The handler was called twice, once for each complete payload.
    verify(mockHandler, times(2)).onPayloadAvailable(any(), eq(true));

    // FIXED: The second packet was exactly 12 bytes and was fully consumed.
    assertEquals(12, readBuffer.position());
  }
}
