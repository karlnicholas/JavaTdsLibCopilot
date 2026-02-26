package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class TdsMessageAssembler {
  private static final Logger logger = LoggerFactory.getLogger(TdsMessageAssembler.class);
  private static final int TDS_HEADER_LENGTH = 8;
  private static final int STATUS_OFFSET = 1;
  private static final int EOM_BIT = 0x01;

  private final List<byte[]> inboundMessageParts = new ArrayList<>();
  private int inboundMessageTotalLength = 0;
  private byte inboundMessageType = 0;
  private short inboundMessageSpid = 0;
  private byte inboundMessagePacketId = 0;

  /**
   * Parses the network buffer. If a complete logical message is formed,
   * it is dispatched to the provided handler.
   */
  public void processNetworkBuffer(ByteBuffer readBuffer, Consumer<TdsMessage> messageHandler) {
    while (readBuffer.remaining() >= TDS_HEADER_LENGTH) {
      int length = Short.toUnsignedInt(readBuffer.getShort(readBuffer.position() + 2));

      if (length > readBuffer.capacity()) {
        throw new IllegalStateException("TDS Packet length exceeds buffer capacity");
      }

      if (readBuffer.remaining() < length) {
        logger.trace("Incomplete TDS packet in buffer. Need {} bytes, have {}", length, readBuffer.remaining());
        break;
      }

      int packetStart = readBuffer.position();
      byte type = readBuffer.get(packetStart);
      byte status = readBuffer.get(packetStart + STATUS_OFFSET);
      short spid = readBuffer.getShort(packetStart + 4);
      byte packetId = readBuffer.get(packetStart + 6);

      logger.trace("Read packet header: Type=0x{}, Status=0x{}, Length={}, PacketId={}",
          String.format("%02X", type), String.format("%02X", status), length, packetId);

      int payloadLen = length - TDS_HEADER_LENGTH;
      byte[] payloadChunk = new byte[payloadLen];

      readBuffer.position(packetStart + TDS_HEADER_LENGTH);
      readBuffer.get(payloadChunk);

      if (inboundMessageParts.isEmpty()) {
        inboundMessageType = type;
        inboundMessageSpid = spid;
        inboundMessagePacketId = packetId;
      }

      inboundMessageParts.add(payloadChunk);
      inboundMessageTotalLength += payloadLen;

      if ((status & EOM_BIT) != 0) {
        logger.trace("EOM flag detected. Assembling logical message of {} bytes.", inboundMessageTotalLength);
        dispatchMessage(status, messageHandler);
      }
    }
  }

  private void dispatchMessage(byte finalStatus, Consumer<TdsMessage> messageHandler) {
    ByteBuffer fullPayload = ByteBuffer.allocate(inboundMessageTotalLength).order(ByteOrder.LITTLE_ENDIAN);
    for (byte[] part : inboundMessageParts) {
      fullPayload.put(part);
    }
    fullPayload.flip();

    TdsMessage logicalMessage = new TdsMessage(
        inboundMessageType, finalStatus, inboundMessageTotalLength + TDS_HEADER_LENGTH,
        inboundMessageSpid, inboundMessagePacketId, fullPayload, System.nanoTime(), null
    );

    inboundMessageParts.clear();
    inboundMessageTotalLength = 0;

    if (messageHandler != null) {
      messageHandler.accept(logicalMessage);
    } else {
      logger.warn("No message handler registered to receive assembled TdsMessage.");
    }
  }
}