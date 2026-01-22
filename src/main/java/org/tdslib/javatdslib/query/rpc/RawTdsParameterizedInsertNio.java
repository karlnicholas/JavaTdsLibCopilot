package org.tdslib.javatdslib.query.rpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class RawTdsParameterizedInsertNio {

  private static final int TDS_PACKET_SIZE = 4096;
  private static final byte TDS_RPC = 0x03;
  private static final byte TDS_STATUS_LAST = 0x01;

  // TDS type tokens
  private static final byte TYPE_NVARCHAR   = (byte) 0xE7;  // NVARCHAR
  private static final byte TYPE_BIGINT     = 0x7F;

  // RPC parameter flags
  private static final byte RPC_PARAM_INPUT = 0x01;

  /**
   * Sends a fully parameterized INSERT via sp_executesql RPC.
   *
   * @param channel   already authenticated SocketChannel
   * @param firstName nullable String (max 100 chars)
   * @param lastName  nullable String (max 100 chars)
   * @param email     non-null String (max 254 chars)
   * @param postCount BIGINT value
   */
  public static void sendInsert(SocketChannel channel,
                                String firstName,
                                String lastName,
                                String email,
                                long postCount) throws IOException {

    ByteBuffer payload = buildRpcPayload(firstName, lastName, email, postCount);
    sendTdsPackets(channel, TDS_RPC, payload);
  }

  private static ByteBuffer buildRpcPayload(String firstName, String lastName, String email, long postCount) {
    ByteBuffer buf = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);

    // ProcID = 10 (sp_executesql)
    buf.putShort((short) 10);

    // RPC flags: 0
    buf.put((byte) 0x00);

    // Param 1: @stmt - the SQL statement
    putParamName(buf, "@stmt");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf,
        "INSERT INTO dbo.users (firstName, lastName, email, postCount) " +
            "VALUES (@p1, @p2, @p3, @p4)");

    // Param 2: @params - parameter declarations
    putParamName(buf, "@params");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf,
        "@p1 nvarchar(100), @p2 nvarchar(100), @p3 nvarchar(254), @p4 bigint");

    // Param 3: @p1 = firstName (NVARCHAR(100))
    putParamName(buf, "@p1");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarchar(buf, 100);
    putPlpUnicodeString(buf, firstName != null ? firstName : "");  // empty string for NULL-ish, or handle NULL separately below

    // Param 4: @p2 = lastName (NVARCHAR(100))
    putParamName(buf, "@p2");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarchar(buf, 100);
    putPlpUnicodeString(buf, lastName != null ? lastName : "");

    // Param 5: @p3 = email (NVARCHAR(254))
    putParamName(buf, "@p3");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarchar(buf, 254);
    putPlpUnicodeString(buf, email);  // assume non-null

    // Param 6: @p4 = postCount (BIGINT)
    putParamName(buf, "@p4");
    buf.put(RPC_PARAM_INPUT);
    buf.put(TYPE_BIGINT);
    buf.put((byte) 0x00);  // no extra type info
    buf.putLong(postCount);

    buf.flip();
    return buf;
  }

  private static void putParamName(ByteBuffer buf, String name) {
    byte[] utf16 = name.getBytes(StandardCharsets.UTF_16LE);
    buf.putShort((short) (utf16.length / 2));   // length in chars
    buf.put(utf16);
  }

  private static void putTypeInfoNVarcharMax(ByteBuffer buf) {
    buf.put(TYPE_NVARCHAR);
    buf.putShort((short) 0xFFFF);       // MAX
    putDummyCollation(buf);
  }

  private static void putTypeInfoNVarchar(ByteBuffer buf, int maxChars) {
    buf.put(TYPE_NVARCHAR);
    buf.putShort((short) (maxChars * 2));  // length in bytes (UTF-16)
    putDummyCollation(buf);
  }

  private static void putDummyCollation(ByteBuffer buf) {
    // Minimal Latin1_General_100_CI_AS-ish
    buf.putInt(0x00000409);     // LCID
    buf.put((byte) 52);         // Sort ID
    buf.putShort((short) 0);    // version/flags
  }

  private static void putPlpUnicodeString(ByteBuffer buf, String value) {
    if (value == null || value.isEmpty()) {
      // For NULL: send PLP with total length 0xFFFFFFFFFFFFFFFF and no chunks
      buf.putInt(-1);                     // PLP marker
      buf.putInt(0);                      // chunk length 0
      buf.putLong(-1L);                   // total length -1 (but actually signals NULL when no data)
      return;
    }

    byte[] utf16 = value.getBytes(StandardCharsets.UTF_16LE);

    buf.putInt(-1);                     // PLP marker 0xFFFFFFFF
    buf.putInt(utf16.length);           // first chunk length
    buf.put(utf16);
    buf.putInt(0);                      // end chunks
    buf.putLong(-1L);                   // total length unknown
  }

  private static void sendTdsPackets(SocketChannel channel, byte packetType, ByteBuffer payload)
      throws IOException {

    payload.rewind();
    int remaining = payload.remaining();
    int packetNumber = 1;

    while (remaining > 0) {
      ByteBuffer packet = ByteBuffer.allocate(TDS_PACKET_SIZE).order(ByteOrder.LITTLE_ENDIAN);

      int chunkSize = Math.min(remaining, TDS_PACKET_SIZE - 8);

      // TDS Header
      packet.put(packetType);
      packet.put(TDS_STATUS_LAST);
      packet.putShort((short) (8 + chunkSize));   // total length
      packet.putShort((short) 0);                 // SPID
      packet.put((byte) packetNumber++);
      packet.put((byte) 0);                       // window

      // Payload chunk
      byte[] chunk = new byte[chunkSize];
      payload.get(chunk);
      packet.put(chunk);

      // Pad to 4096
      while (packet.position() < TDS_PACKET_SIZE) {
        packet.put((byte) 0);
      }

      packet.flip();
      while (packet.hasRemaining()) {
        channel.write(packet);
      }

      remaining -= chunkSize;
    }
  }

  // Example usage:
    /*
    SocketChannel channel = ...; // authenticated
    try {
        sendInsert(channel,
                   "Nicholas",
                   "Rivera",
                   "nick@example.com",
                   1L);
        // read response loop here...
    } catch (IOException e) {
        e.printStackTrace();
    }
    */
}