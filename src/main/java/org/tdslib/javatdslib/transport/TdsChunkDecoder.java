package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Strips 8-byte TDS headers from the network stream and forwards the raw payload
 * chunks directly to the TdsStreamHandler.
 */
public class TdsChunkDecoder {
  private static final Logger logger = LoggerFactory.getLogger(TdsChunkDecoder.class);
  private static final int TDS_HEADER_LENGTH = 8;
  private static final int STATUS_OFFSET = 1;
  private static final int EOM_BIT = 0x01;

  private final TdsStreamHandler streamHandler;

  private int currentChunkBytesRemaining = 0;
  private boolean currentChunkIsEom = false;

  public TdsChunkDecoder(TdsStreamHandler streamHandler) {
    this.streamHandler = streamHandler;
  }

  public void decode(ByteBuffer networkBuffer) {
    while (networkBuffer.hasRemaining()) {

      // 1. Parse the 8-byte frame if we are at the start of a new chunk
      if (currentChunkBytesRemaining == 0) {
        if (networkBuffer.remaining() < TDS_HEADER_LENGTH) {
          return; // Wait for the rest of the header to arrive
        }

        int headerStart = networkBuffer.position();
        byte status = networkBuffer.get(headerStart + STATUS_OFFSET);
        int length = Short.toUnsignedInt(networkBuffer.getShort(headerStart + 2));

        networkBuffer.position(headerStart + TDS_HEADER_LENGTH); // Skip header
        currentChunkBytesRemaining = length - TDS_HEADER_LENGTH;
        currentChunkIsEom = (status & EOM_BIT) != 0;
        networkBuffer.position(headerStart + TDS_HEADER_LENGTH); // Skip header
        currentChunkBytesRemaining = length - TDS_HEADER_LENGTH;
        currentChunkIsEom = (status & EOM_BIT) != 0;

        // ADD THIS
        logger.trace("DECODER: Stripped header. Payload size: {}, Is EOM: {}",
            currentChunkBytesRemaining, currentChunkIsEom);
      }

      // 2. Stream the payload directly to the parser
      if (currentChunkBytesRemaining > 0) {
        int bytesAvailable = networkBuffer.remaining();
        int bytesToRead = Math.min(bytesAvailable, currentChunkBytesRemaining);

        // Create a protected window (slice) so the parser can't read past the current chunk boundary
        int originalLimit = networkBuffer.limit();
        networkBuffer.limit(networkBuffer.position() + bytesToRead);
        ByteBuffer payloadSlice = networkBuffer.slice();

        // Hand over to the Stateful Token Parser
        int preConsumePosition = payloadSlice.position();
        streamHandler.onPayloadAvailable(payloadSlice, currentChunkIsEom);
        int bytesConsumed = payloadSlice.position() - preConsumePosition;

        // Advance our main buffer by exactly what the parser actually consumed
        networkBuffer.limit(originalLimit);
        networkBuffer.position(networkBuffer.position() + bytesConsumed);
        currentChunkBytesRemaining -= bytesConsumed;

        // If the parser didn't consume the whole slice, it means it triggered suspendRead()
        if (payloadSlice.hasRemaining()) {
          return;
        }
      }
    }
  }
}