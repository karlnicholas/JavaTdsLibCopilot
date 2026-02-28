package org.tdslib.javatdslib.handshake;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.PreLoginResponse;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.payloads.prelogin.PreLoginPayload;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * Handles the Pre-Login phase of the TDS connection process.
 * Negotiates connection properties such as encryption and packet size
 * before the main Login7 phase begins.
 */
public class PreLoginPhase {
  private static final Logger logger = LoggerFactory.getLogger(PreLoginPhase.class);

  /**
   * Executes the Pre-Login handshake with the SQL Server.
   *
   * @param transport The TDS transport layer for sending/receiving messages.
   * @return The parsed PreLoginResponse from the server.
   * @throws Exception If an I/O or protocol error occurs.
   */
  public PreLoginResponse execute(TdsTransport transport) throws Exception {
    logger.debug("Starting Pre-Login phase");
    PreLoginPayload preLoginPayload = new PreLoginPayload(false);

    TdsMessage preLoginMsg = TdsMessage.createRequest(
        PacketType.PRE_LOGIN.getValue(),
        preLoginPayload.buildBuffer()
    );
    transport.sendMessageDirect(preLoginMsg);

    List<TdsMessage> preLoginResponses = transport.receiveFullResponse();
    return processPreLoginResponse(preLoginResponses);
  }

  private PreLoginResponse processPreLoginResponse(List<TdsMessage> packets) {
    ByteBuffer combined = combinePayloads(packets);
    PreLoginResponse response = new PreLoginResponse();

    if (combined.hasRemaining()) {
      while (combined.hasRemaining()) {
        byte option = combined.get();
        if (option == (byte) 0xFF) {
          break; // Terminator byte: End of Option Headers
        }

        short offset = combined.getShort();
        short length = combined.getShort();
        int savedPos = combined.position();
        combined.position(offset);

        switch (option) {
          case 0x00: // VERSION
            int major = combined.get() & 0xFF;
            int minor = combined.get() & 0xFF;

            // The build number inside the payload is Little-Endian
            combined.order(ByteOrder.LITTLE_ENDIAN);
            short build = combined.getShort();
            combined.order(ByteOrder.BIG_ENDIAN); // Restore for next header offsets

            response.setVersion(major, minor, build);
            break;

          case 0x01: // ENCRYPTION
            response.setEncryption(combined.get());
            break;

          case 0x02: // INSTANCE
            logger.debug("Received INSTANCE option in Pre-Login response.");
            break;

          case 0x03: // THREADID
            logger.debug("Received THREADID option in Pre-Login response.");
            break;

          case 0x04: // PACKETSIZE
            int psLen = combined.get() & 0xFF;
            byte[] psBytes = new byte[psLen];
            combined.get(psBytes);
            try {
              String psString = new String(psBytes, StandardCharsets.US_ASCII);
              response.setNegotiatedPacketSize(Integer.parseInt(psString));
            } catch (NumberFormatException e) {
              logger.error("Failed to parse negotiated packet size");
            }
            break;

          case 0x05: // MARS
            logger.debug("Received MARS option in Pre-Login response.");
            break;

          default:
            logger.debug("Unknown or ignored Pre-Login option: 0x{}",
                String.format("%02X", option));
            break;
        }
        combined.position(savedPos);
      }
    }
    return response;
  }

  private ByteBuffer combinePayloads(List<TdsMessage> packets) {
    int total = packets.stream().mapToInt(m -> m.getPayload().remaining()).sum();
    ByteBuffer combined = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
    for (TdsMessage m : packets) {
      combined.put(m.getPayload().duplicate());
    }
    combined.flip();
    return combined;
  }
}