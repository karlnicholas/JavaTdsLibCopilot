package org.tdslib.javatdslib.payloads.login7;

import org.tdslib.javatdslib.payloads.Payload;
import org.tdslib.javatdslib.payloads.login7.auth.FedAuth;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Builds a TDS LOGIN7 payload buffer.
 */
public final class Login7Payload extends Payload {
  private static final byte FeatureExtensionTerminator = (byte) 0xFF;
  private static final int ClientIdSize = 6;
  private static final int FIXED_HEADER_SIZE = 94; // TDS 7.x fixed header size

  public Login7Options options;
  public OptionFlags1 optionFlags1;
  public OptionFlags2 optionFlags2;
  public OptionFlags3 optionFlags3;
  public TypeFlags typeFlags;

  public String username;
  public String password;
  public String serverName;
  public String appName;
  public String hostname;
  public String libraryName;
  public String language;
  public String database;

  public byte[] clientId;
  public ByteBuffer sspi;
  public String attachDbFile;
  public String changePassword;
  public FedAuth fedAuth;

  /**
   * Construct Login7Payload with specified options.
   *
   * @param options login options (null -> defaults)
   */
  public Login7Payload(final Login7Options options) {
    this.options = options == null ? new Login7Options() : options;
    this.optionFlags1 = new OptionFlags1();
    this.optionFlags2 = new OptionFlags2();
    this.optionFlags3 = new OptionFlags3();
    this.typeFlags = new TypeFlags();
    this.libraryName = "TdsLib";

    // Generate random ClientID if null
    if (this.clientId == null) {
      this.clientId = new byte[ClientIdSize];
      Random r = new Random();
      r.nextBytes(this.clientId);
    }

    buildBufferInternal();
  }

  /**
   * Helper class to track Offset, Length, and Data for variable fields.
   */
  private static class FieldRef {
    final int offset;
    final int len; // Length in characters (or bytes for binary)
    final byte[] data;

    FieldRef(final byte[] data, final boolean isChar, final int currentOffset) {
      this.offset = currentOffset;
      this.data = data;
      if (isChar) {
        // Length expressed in characters for UNICODE fields (2 bytes/char)
        this.len = data.length / 2;
      } else {
        // Binary fields length in bytes
        this.len = data.length;
      }
    }
  }

  @Override
  protected void buildBufferInternal() {
    // 1. Prepare all Variable Length Data (Converted to byte arrays)
    final byte[] hostBytes = toBytes(hostname);
    final byte[] userBytes = toBytes(username);
    final byte[] passBytes = scramblePassword(toBytes(password));
    final byte[] appBytes = toBytes(appName);
    final byte[] serverBytes = toBytes(serverName);
    final byte[] extBytes = getExtensionsBytes(); // Extensions (might be empty)
    final byte[] libBytes = toBytes(libraryName);
    final byte[] langBytes = toBytes(language);
    final byte[] dbBytes = toBytes(database);

    final byte[] attachBytes = toBytes(attachDbFile);
    final byte[] changePassBytes = scramblePassword(toBytes(changePassword));

    final byte[] sspiBytes;
    if (sspi != null && sspi.hasRemaining()) {
      sspiBytes = toBytes(sspi);
    } else {
      sspiBytes = new byte[0];
    }

    // CRITICAL FIX: Update OptionFlags3 based on whether extensions exist
    if (extBytes.length > 0) {
      optionFlags3.setExtensionUsed(true);
    } else {
      optionFlags3.setExtensionUsed(false);
    }

    // 2. Calculate Offsets (Strict TDS Order)
    int currentOffset = FIXED_HEADER_SIZE;

    final FieldRef refHost = new FieldRef(hostBytes, true, currentOffset);
    currentOffset += hostBytes.length;

    final FieldRef refUser = new FieldRef(userBytes, true, currentOffset);
    currentOffset += userBytes.length;

    final FieldRef refPass = new FieldRef(passBytes, true, currentOffset);
    currentOffset += passBytes.length;

    final FieldRef refApp = new FieldRef(appBytes, true, currentOffset);
    currentOffset += appBytes.length;

    final FieldRef refServer = new FieldRef(serverBytes, true, currentOffset);
    currentOffset += serverBytes.length;

    final FieldRef refExt = new FieldRef(extBytes, false, currentOffset); // Binary
    currentOffset += extBytes.length;

    final FieldRef refLib = new FieldRef(libBytes, true, currentOffset);
    currentOffset += libBytes.length;

    final FieldRef refLang = new FieldRef(langBytes, true, currentOffset);
    currentOffset += langBytes.length;

    final FieldRef refDb = new FieldRef(dbBytes, true, currentOffset);
    currentOffset += dbBytes.length;

    // Note: ClientID is inside fixed header, no offset needed.

    final FieldRef refSspi = new FieldRef(sspiBytes, false, currentOffset);
    currentOffset += sspiBytes.length;

    final FieldRef refAttach = new FieldRef(attachBytes, true, currentOffset);
    currentOffset += attachBytes.length;

    final FieldRef refChange = new FieldRef(changePassBytes, true, currentOffset);
    currentOffset += changePassBytes.length;

    // 3. Allocate Buffer
    // Total Size = currentOffset (Head + Data)
    this.buffer = ByteBuffer.allocate(currentOffset).order(ByteOrder.LITTLE_ENDIAN);

    // 4. Write Fixed Header (94 bytes)

    // [0-3] Total Length (CRITICAL: Must include this length field itself)
    buffer.putInt(currentOffset);

    // [4-35] Standard Options
    buffer.putInt(options.getTdsVersion().getValue());
    buffer.putInt(options.getPacketSize());
    buffer.putInt((int) options.getClientProgVer());
    buffer.putInt((int) options.getClientPid());
    buffer.putInt((int) options.getConnectionId());
    buffer.put(optionFlags1.toByte());
    buffer.put(optionFlags2.toByte());
    buffer.put(typeFlags.toByte());
    buffer.put(optionFlags3.toByte());
    buffer.putInt(options.getClientTimeZone());
    buffer.putInt((int) options.getClientLcid());

    // [36-86] Offsets/Lengths (Order is Strict)
    writeOffLen(buffer, refHost);
    writeOffLen(buffer, refUser);
    writeOffLen(buffer, refPass);
    writeOffLen(buffer, refApp);
    writeOffLen(buffer, refServer);
    writeOffLen(buffer, refExt); // Extensions
    writeOffLen(buffer, refLib);
    writeOffLen(buffer, refLang);
    writeOffLen(buffer, refDb);

    // [80-85] Client ID (6 bytes)
    if (clientId.length == ClientIdSize) {
      buffer.put(clientId);
    } else {
      // Ensure exactly 6 bytes in clientId
      byte[] local = new byte[ClientIdSize];
      System.arraycopy(clientId, 0, local, 0, Math.min(clientId.length, ClientIdSize));
      buffer.put(local);
    }

    writeOffLen(buffer, refSspi);
    writeOffLen(buffer, refAttach);
    writeOffLen(buffer, refChange);

    // [90-93] SSPI Long Length (4 bytes)
    buffer.putInt(0);

    // 5. Write Variable Data (Order matches offset calculations)
    buffer.put(hostBytes);
    buffer.put(userBytes);
    buffer.put(passBytes);
    buffer.put(appBytes);
    buffer.put(serverBytes);
    buffer.put(extBytes);
    buffer.put(libBytes);
    buffer.put(langBytes);
    buffer.put(dbBytes);
    buffer.put(sspiBytes);
    buffer.put(attachBytes);
    buffer.put(changePassBytes);

    buffer.flip();
  }

  // --- Helpers ---

  private void writeOffLen(final ByteBuffer buf, final FieldRef field) {
    buf.putShort((short) field.offset);
    buf.putShort((short) field.len);
  }

  private byte[] toBytes(final String s) {
    if (s == null) {
      return new byte[0];
    }
    return s.getBytes(StandardCharsets.UTF_16LE);
  }

  private byte[] toBytes(final ByteBuffer b) {
    byte[] arr = new byte[b.remaining()];
    b.slice().get(arr);
    return arr;
  }

  private byte[] getExtensionsBytes() {
    List<ByteBuffer> buffers = new ArrayList<>();
    boolean hasExtensions = false;

    if (fedAuth != null) {
      // Example: convert fedAuth to its extension buffer(s)
      ByteBuffer fb = fedAuth.toByteBuffer();
      if (fb != null && fb.hasRemaining()) {
        buffers.add(fb);
        hasExtensions = true;
      }
    }

    // Only write extensions block if we actually have extensions
    if (hasExtensions) {
      // Determine total length including terminator
      int total = 0;
      for (int i = 0; i < buffers.size(); i++) {
        ByteBuffer b = buffers.get(i);
        total += b.remaining();
      }

      // add terminator byte
      total += 1;
      byte[] out = new byte[total];
      int pos = 0;
      for (int i = 0; i < buffers.size(); i++) {
        ByteBuffer bb = buffers.get(i);
        byte[] tmp = toBytes(bb);
        System.arraycopy(tmp, 0, out, pos, tmp.length);
        pos += tmp.length;
      }

      // terminator
      out[pos] = FeatureExtensionTerminator;
      return out;
    }

    return new byte[0];
  }

  /**
   * Scrambles (obfuscates) a password byte array for use in TDS LOGIN7 packet.
   * This is the exact algorithm specified in [MS-TDS] for SQL Server Authentication.
   *
   * @param data The original password encoded as UTF-16LE bytes
   * @return The scrambled (obfuscated) byte array to send over the wire
   */
  private byte[] scramblePassword(final byte[] data) {
    if (data == null || data.length == 0) {
      return new byte[0];
    }

    // The length must be even (UTF-16LE characters)
    if (data.length % 2 != 0) {
      // pad with zero to make even length
      byte[] tmp = new byte[data.length + 1];
      System.arraycopy(data, 0, tmp, 0, data.length);
      return scramblePassword(tmp);
    }

    byte[] result = new byte[data.length];

    for (int i = 0; i < data.length; i++) {
      // 1. Mask to unsigned int (0-255) to prevent sign extension
      int b = data[i] & 0xFF;

      // 2. Swap Nibbles:
      //    (b >>> 4) moves high nibble to low (unsigned shift)
      //    (b << 4)  moves low nibble to high
      int swapped = (b >>> 4) | (b << 4);

      // 3. XOR with 0xA5 and cast back to byte
      result[i] = (byte) (swapped ^ 0xA5);
    }
    return result;
  }
}
