package org.tdslib.javatdslib;

import org.tdslib.javatdslib.messages.Message;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class PreLoginRequest {
    private boolean encrypt = false;          // ENCRYPTION option
    private boolean trustServerCert = false;  // TRUSTSERVERCERTIFICATE
    private String instanceName = "";         // INSTANCE
    private boolean mars = false;             // MARS support

    // Setters (fluent style)
    public PreLoginRequest withEncryption(boolean encrypt) {
        this.encrypt = encrypt;
        return this;
    }

    public PreLoginRequest withTrustServerCertificate(boolean trust) {
        this.trustServerCert = trust;
        return this;
    }

    public PreLoginRequest withInstance(String instance) {
        this.instanceName = Objects.requireNonNullElse(instance, "");
        return this;
    }

    public PreLoginRequest withMars(boolean mars) {
        this.mars = mars;
        return this;
    }

    public Message toMessage() {
        // PreLogin payload structure: option list + data block
        // Each option: byte type, short offset, short length

        ByteBuffer data = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);

        // We'll collect offsets while writing data
        int[] offsets = new int[6];
        int[] lengths = new int[6];

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
            byte[] bytes = (instanceName + "\0").getBytes(java.nio.charset.StandardCharsets.US_ASCII);
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

        ByteBuffer payload = ByteBuffer.allocate(headerSize + data.position()).order(ByteOrder.LITTLE_ENDIAN);

        // Option count
        payload.putShort((short) optionCount);

        // Option entries: type, offset, length
        payload.put((byte) 0x01); payload.putShort((short) offsets[0]); payload.putShort((short) lengths[0]);
        payload.put((byte) 0x02); payload.putShort((short) offsets[1]); payload.putShort((short) lengths[1]);
        payload.put((byte) 0x03); payload.putShort((short) offsets[2]); payload.putShort((short) lengths[2]);
        payload.put((byte) 0x04); payload.putShort((short) offsets[3]); payload.putShort((short) lengths[3]);
        payload.put((byte) 0x05); payload.putShort((short) offsets[4]); payload.putShort((short) lengths[4]);
        payload.put((byte) 0xFF); // terminator

        // Append the data block
        data.flip();
        payload.put(data);

        payload.flip();

        return new Message(
                (byte) 0x12,           // PreLogin
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