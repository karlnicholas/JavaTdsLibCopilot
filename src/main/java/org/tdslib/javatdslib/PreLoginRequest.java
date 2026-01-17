package org.tdslib.javatdslib;

import org.tdslib.javatdslib.transport.Message;
import org.tdslib.javatdslib.packets.PacketType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Build a TDS PreLogin request message.
 *
 * <p>Supports setting common pre-login options such as encryption, trust of the
 * server certificate, instance name, and MARS support. The {@link #toMessage()}
 * method produces a framed {@link Message} containing the PreLogin payload.
 */
public class PreLoginRequest {
  private boolean encrypt = false;          // ENCRYPTION option
  private boolean trustServerCert = false;  // TRUSTSERVERCERTIFICATE
  private String instanceName = "";         // INSTANCE
  private boolean mars = false;             // MARS support

  /**
   * Enable or disable encryption for the PreLogin request.
   *
   * @param encrypt true to enable encryption
   * @return this request for chaining
   */
  public PreLoginRequest withEncryption(boolean encrypt) {
    this.encrypt = encrypt;
    return this;
  }

  /**
   * Set whether to trust the server certificate (TRUSTSERVERCERTIFICATE).
   *
   * @param trust true to trust the server certificate
   * @return this request for chaining
   */
  public PreLoginRequest withTrustServerCertificate(boolean trust) {
    this.trustServerCert = trust;
    return this;
  }

  /**
   * Set the SQL Server instance name to include in the PreLogin payload.
   *
   * @param instance instance name, null treated as empty string
   * @return this request for chaining
   */
  public PreLoginRequest withInstance(String instance) {
    this.instanceName = Objects.requireNonNullElse(instance, "");
    return this;
  }

  /**
   * Enable or disable MARS (Multiple Active Result Sets) support.
   *
   * @param mars true to enable MARS
   * @return this request for chaining
   */
  public PreLoginRequest withMars(boolean mars) {
    this.mars = mars;
    return this;
  }

  /**
   * Construct the PreLogin message payload and wrap it into a {@link Message}.
   *
   * @return framed PreLogin {@link Message}
   */
  public Message toMessage() {
    // PreLogin payload structure: option list + data block
    // Each option: byte type, short offset, short length

    ByteBuffer data = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);

    // We'll collect offsets while writing data
    final int[] offsets = new int[6];
    final int[] lengths = new int[6];

    // 0x01 - VERSION (always sent, fixed 6 bytes)
    offsets[0] = data.position();
    data.put((byte) 0x09).put((byte) 0x00).put((byte) 0x00).put((byte) 0x00); // TDS 7.4
    data.putShort((short) 0x0000).put((byte) 0x00); // sub-version
    lengths[0] = 6;

    // 0x02 - ENCRYPTION
    offsets[1] = data.position();
    data.put(encrypt ? (byte) 0x01 : (byte) 0x00); // 0=off, 1=on, 2=requested
    lengths[1] = 1;

    // 0x03 - INSTANCENAME (null-terminated string)
    offsets[2] = data.position();
    if (!instanceName.isEmpty()) {
      byte[] bytes = (instanceName + "\0")
          .getBytes(StandardCharsets.US_ASCII);
      data.put(bytes);
      lengths[2] = bytes.length;
    } else {
      lengths[2] = 0;
    }

    // 0x04 - THREADID (usually 0)
    offsets[3] = data.position();
    data.putInt(0);
    lengths[3] = 4;

    // 0x05 - MARS (1 byte)
    offsets[4] = data.position();
    data.put(mars ? (byte) 0x01 : (byte) 0x00);
    lengths[4] = 1;

    // 0xFF - terminator
    offsets[5] = data.position();
    lengths[5] = 0;

    // Now build the final payload: options table + data
    int optionCount = 5; // VERSION + ENCRYPTION + INSTANCE + THREADID + MARS
    int headerSize = 2 + optionCount * 5 + 1; // 2-byte count + each 5 bytes + 0xFF

    int payloadSize = headerSize + data.position();
    ByteBuffer payload = ByteBuffer.allocate(payloadSize).order(ByteOrder.LITTLE_ENDIAN);

    // Option count
    payload.putShort((short) optionCount);

    // Option entries: type, offset, length
    payload.put((byte) 0x01);
    payload.putShort((short) offsets[0]);
    payload.putShort((short) lengths[0]);
    payload.put((byte) 0x02);
    payload.putShort((short) offsets[1]);
    payload.putShort((short) lengths[1]);
    payload.put((byte) 0x03);
    payload.putShort((short) offsets[2]);
    payload.putShort((short) lengths[2]);
    payload.put((byte) 0x04);
    payload.putShort((short) offsets[3]);
    payload.putShort((short) lengths[3]);
    payload.put((byte) 0x05);
    payload.putShort((short) offsets[4]);
    payload.putShort((short) lengths[4]);
    payload.put((byte) 0xFF); // terminator

    // Append the data block
    data.flip();
    payload.put(data);

    payload.flip();

    return new Message(
        PacketType.PRE_LOGIN.getValue(),           // PreLogin
        (byte) 0x01,           // EOM
        payload.capacity() + 8,
        (short) 0,
        (short) 1,
        payload,
        System.nanoTime(),
        null
    );
  }
}
