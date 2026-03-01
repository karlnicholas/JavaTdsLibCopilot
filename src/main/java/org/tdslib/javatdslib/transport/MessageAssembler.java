package org.tdslib.javatdslib.transport;

import org.tdslib.javatdslib.packets.TdsMessage;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Abstracts the assembly of incoming network byte chunks into logical TdsMessages.
 */
public interface MessageAssembler {

  /**
   * Parses the incoming network buffer and dispatches complete logical messages to the handler.
   *
   * @param readBuffer     The buffer containing raw bytes from the network.
   * @param messageHandler The callback to invoke when a logical message is fully assembled.
   */
  void processNetworkBuffer(ByteBuffer readBuffer, Consumer<TdsMessage> messageHandler);
}