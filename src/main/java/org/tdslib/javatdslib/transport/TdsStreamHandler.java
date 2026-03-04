package org.tdslib.javatdslib.transport;

import java.nio.ByteBuffer;

/**
 * Handles continuous chunks of TDS payload bytes as they arrive from the network.
 */
public interface TdsStreamHandler {
  /**
   * Called when a chunk of TDS payload is available.
   * The handler must advance the ByteBuffer's position by the number of bytes it consumes.
   *
   * @param payload A buffer representing the current chunk.
   * @param isEom   True if this is the final chunk of the logical message.
   */
  void onPayloadAvailable(ByteBuffer payload, boolean isEom);
}