package org.tdslib.javatdslib.payloads.prelogin;

import org.tdslib.javatdslib.payloads.Payload;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PreLogin payload.
 */
public class PreLoginPayload extends Payload {
  private EncryptionType encryption;
  private SqlVersion version;
  private byte instance;
  private long threadId;
  private byte mars;
  private byte fedAuth;

  // --- Getters / Setters ---

  public EncryptionType getEncryption() {
    return encryption;
  }

  public void setEncryption(final EncryptionType encryption) {
    this.encryption = encryption;
  }

  public SqlVersion getVersion() {
    return version;
  }

  public void setVersion(final SqlVersion version) {
    this.version = version;
  }

  public byte getInstance() {
    return instance;
  }

  public void setInstance(final byte instance) {
    this.instance = instance;
  }

  public long getThreadId() {
    return threadId;
  }

  public void setThreadId(final long threadId) {
    this.threadId = threadId;
  }

  public byte getMars() {
    return mars;
  }

  public void setMars(final byte mars) {
    this.mars = mars;
  }

  public byte getFedAuth() {
    return fedAuth;
  }

  public void setFedAuth(final byte fedAuth) {
    this.fedAuth = fedAuth;
  }

  /**
   * Construct a PreLoginPayload to send.
   *
   * @param encrypt whether to request encryption
   */
  public PreLoginPayload(final boolean encrypt) {
    this.encryption = encrypt ? EncryptionType.ON : EncryptionType.OFF;
    this.version = new SqlVersion(16, 0, 0, 0);
    this.instance = 0;
    this.threadId = Thread.currentThread().getId();
    this.mars = 0;
    this.fedAuth = 0;
    buildBufferInternal();
  }

  /**
   * Construct a PreLoginPayload from an existing buffer.
   *
   * @param buffer source buffer containing PreLogin payload
   */
  public PreLoginPayload(final ByteBuffer buffer) {
    this.buffer = buffer;
    extractBufferData();
  }

  @Override
  protected void buildBufferInternal() {
    // 1. Prepare Data Blocks (Little Endian)
    byte[] versionBytes = version != null ? version.toBytes() : new byte[6];
    final byte encryptionValue = encryption != null ? encryption.getValue() : (byte) 0;
    final byte[] encryptionBytes = new byte[]{encryptionValue};
    byte[] instanceBytes = new byte[]{instance};

    byte[] threadIdBytes = new byte[4];
    // Write ThreadID as little-endian into a 4-byte array
    ByteBuffer tmp = ByteBuffer.wrap(threadIdBytes);
    tmp.order(ByteOrder.LITTLE_ENDIAN);
    tmp.putInt((int) threadId);

    byte[] marsBytes = new byte[]{mars};
    byte[] fedAuthBytes = new byte[]{fedAuth};

    // 2. Define Options List (Tag, Data)
    // Using a dynamic list ensures header count matches actual data written
    List<Object[]> options = new ArrayList<>();

    options.add(new Object[]{(byte) 0x00, versionBytes});    // Version
    options.add(new Object[]{(byte) 0x01, encryptionBytes}); // Encryption
    //        options.add(new Object[] { (byte) 0x02, instanceBytes });   // Instance
    //        options.add(new Object[] { (byte) 0x03, threadIdBytes });   // ThreadID
    //        options.add(new Object[] { (byte) 0x04, marsBytes });       // MARS
    //        options.add(new Object[] { (byte) 0x06, fedAuthBytes });    // FedAuth

    // 3. Calculate Sizes
    // Header is: (5 bytes * count) + 1 byte for Terminator
    int headerLen = (options.size() * 5) + 1;

    int dataLen = 0;
    for (Object[] opt : options) {
      dataLen += ((byte[]) opt[1]).length;
    }

    ByteBuffer buf = ByteBuffer.allocate(headerLen + dataLen);

    // 4. Write Headers (Offsets are Big Endian)
    int currentOffset = headerLen;

    for (Object[] opt : options) {
      byte tag = (byte) opt[0];
      byte[] data = (byte[]) opt[1];

      buf.put(tag);
      // Manually write Short as Big Endian to avoid using a Utility or switching buffer order
      buf.putShort((short) currentOffset);
      buf.putShort((short) data.length);

      currentOffset += data.length;
    }

    // Write Terminator
    buf.put((byte) 0xFF);

    // 5. Write Data Bodies
    for (Object[] opt : options) {
      buf.put((byte[]) opt[1]);
    }

    buf.flip();
    this.buffer = buf;
  }

  private void extractBufferData() {
    if (buffer == null) {
      return;
    }

    int oldPos = buffer.position();

    try {
      Map<Byte, Integer> offsets = new HashMap<>();
      Map<Byte, Integer> lengths = new HashMap<>();
      int headerEnd = -1;

      // 1. Read Headers
      // Warning: PreLogin Headers are BIG ENDIAN, but Packet Data is LITTLE ENDIAN.
      // We cannot rely on buffer.getShort(). We must read bytes manually.
      while (buffer.remaining() >= 5) {
        byte tag = buffer.get();
        if ((tag & 0xFF) == 0xFF) {
          headerEnd = buffer.position();
          break;
        }

        // Read 2 bytes for Offset (Big Endian)
        int offHigh = buffer.get() & 0xFF;
        int offLow = buffer.get() & 0xFF;
        int offset = (offHigh << 8) | offLow;

        // Read 2 bytes for Length (Big Endian)
        int lenHigh = buffer.get() & 0xFF;
        int lenLow = buffer.get() & 0xFF;
        int length = (lenHigh << 8) | lenLow;

        offsets.put(tag, offset);
        lengths.put(tag, length);
      }

      if (headerEnd < 0) {
        return;
      }

      // 2. Read Values
      // For reading the data payload (like ThreadID), we want Little Endian.
      // Packet.getData() returns LE, so we can use standard get methods here.

      for (Map.Entry<Byte, Integer> entry : offsets.entrySet()) {
        byte tag = entry.getKey();
        int off = entry.getValue();
        int len = lengths.get(tag);

        buffer.position(off);

        switch (tag) {
          case 0x00: // Version
            if (len == 6) {
              byte[] v = new byte[6];
              buffer.get(v);
              this.version = SqlVersion.fromBytes(v);
            }
            break;
          case 0x01: // Encryption
            if (len == 1) {
              this.encryption = EncryptionType.fromValue(buffer.get());
            }
            break;
          case 0x02: // Instance
            if (len == 1) {
              this.instance = buffer.get();
            }
            break;
          case 0x03: // ThreadId
            if (len == 4) {
              int tid = buffer.getInt();
              this.threadId = tid & 0xFFFFFFFFL;
            }
            break;
          case 0x04: // Mars
            if (len == 1) {
              this.mars = buffer.get();
            }
            break;
          case 0x06: // FedAuth
            if (len == 1) {
              this.fedAuth = buffer.get();
            }
            break;
          default:
            // Unknown option: ignore
            break;
        }
      }
    } finally {
      buffer.position(oldPos);
    }
  }
}
