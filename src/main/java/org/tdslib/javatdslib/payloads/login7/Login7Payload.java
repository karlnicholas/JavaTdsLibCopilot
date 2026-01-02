// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads.login7;

import org.tdslib.javatdslib.payloads.Payload;
import org.tdslib.javatdslib.payloads.login7.auth.FedAuth;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

    public Login7Payload(Login7Options options) {
        this.options = options == null ? new Login7Options() : options;
        this.optionFlags1 = new OptionFlags1();
        this.optionFlags2 = new OptionFlags2();
        this.optionFlags3 = new OptionFlags3();
        this.typeFlags = new TypeFlags();
        this.libraryName = "TdsLib";

        // Generate random ClientID if null
        if (this.clientId == null) {
            this.clientId = new byte[ClientIdSize];
            new Random().nextBytes(this.clientId);
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

        FieldRef(byte[] data, boolean isChar, int currentOffset) {
            this.data = data;
            if (data == null || data.length == 0) {
                this.offset = 0;
                this.len = 0;
            } else {
                this.offset = currentOffset;
                this.len = isChar ? data.length / 2 : data.length;
            }
        }
    }

    @Override
    protected void buildBufferInternal() {
        // 1. Prepare all Variable Length Data (Converted to byte arrays)
        byte[] hostBytes = toBytes(hostname);
        byte[] userBytes = toBytes(username);
        byte[] passBytes = scramblePassword(toBytes(password));
        byte[] appBytes = toBytes(appName);
        byte[] serverBytes = toBytes(serverName);
        byte[] extBytes = getExtensionsBytes(); // Extensions (might be empty)
        byte[] libBytes = toBytes(libraryName);
        byte[] langBytes = toBytes(language);
        byte[] dbBytes = toBytes(database);

        byte[] attachBytes = toBytes(attachDbFile);
        byte[] changePassBytes = scramblePassword(toBytes(changePassword));
        byte[] sspiBytes = (sspi != null && sspi.hasRemaining()) ? toBytes(sspi) : new byte[0];

        // CRITICAL FIX: Update OptionFlags3 based on whether extensions exist
        if (extBytes.length > 0) {
            optionFlags3.setExtensionUsed(true);
        } else {
            optionFlags3.setExtensionUsed(false);
        }

        // 2. Calculate Offsets (Strict TDS Order)
        int currentOffset = FIXED_HEADER_SIZE;

        FieldRef refHost = new FieldRef(hostBytes, true, currentOffset);      currentOffset += hostBytes.length;
        FieldRef refUser = new FieldRef(userBytes, true, currentOffset);      currentOffset += userBytes.length;
        FieldRef refPass = new FieldRef(passBytes, true, currentOffset);      currentOffset += passBytes.length;
        FieldRef refApp = new FieldRef(appBytes, true, currentOffset);        currentOffset += appBytes.length;
        FieldRef refServer = new FieldRef(serverBytes, true, currentOffset);  currentOffset += serverBytes.length;
        FieldRef refExt = new FieldRef(extBytes, false, currentOffset);       currentOffset += extBytes.length; // Binary
        FieldRef refLib = new FieldRef(libBytes, true, currentOffset);        currentOffset += libBytes.length;
        FieldRef refLang = new FieldRef(langBytes, true, currentOffset);      currentOffset += langBytes.length;
        FieldRef refDb = new FieldRef(dbBytes, true, currentOffset);          currentOffset += dbBytes.length;

        // Note: ClientID is inside fixed header, no offset needed.

        FieldRef refSSPI = new FieldRef(sspiBytes, false, currentOffset);     currentOffset += sspiBytes.length;
        FieldRef refAttach = new FieldRef(attachBytes, true, currentOffset);  currentOffset += attachBytes.length;
        FieldRef refChange = new FieldRef(changePassBytes, true, currentOffset); currentOffset += changePassBytes.length;

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
        if (clientId.length == 6) {
            buffer.put(clientId);
        } else {
            buffer.put(new byte[6]);
        }

        writeOffLen(buffer, refSSPI);
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

    private void writeOffLen(ByteBuffer buf, FieldRef field) {
        buf.putShort((short) field.offset);
        buf.putShort((short) field.len);
    }

    private byte[] toBytes(String s) {
        if (s == null) return new byte[0];
        return s.getBytes(StandardCharsets.UTF_16LE);
    }

    private byte[] toBytes(ByteBuffer b) {
        byte[] arr = new byte[b.remaining()];
        b.slice().get(arr);
        return arr;
    }

    private byte[] getExtensionsBytes() {
        List<ByteBuffer> buffers = new ArrayList<>();
        boolean hasExtensions = false;

        if (fedAuth != null) {
            ByteBuffer b = fedAuth.getBuffer();
            if (b != null) {
                buffers.add(b);
                hasExtensions = true;
            }
        }

        // Only write extensions block if we actually have extensions
        if (hasExtensions) {
            buffers.add(ByteBuffer.wrap(new byte[] { FeatureExtensionTerminator }));

            int len = 0;
            for (ByteBuffer b : buffers) len += b.remaining();

            ByteBuffer out = ByteBuffer.allocate(len);
            for (ByteBuffer b : buffers) out.put(b.slice());
            out.flip();
            return toBytes(out);
        } else {
            return new byte[0];
        }
    }

//    private byte[] scramblePassword(byte[] data) {
//        if (data.length == 0) return data;
//
//        for (int i = 0; i < data.length; i++) {
//            // 1. Mask to unsigned int (0-255) to prevent sign extension
//            int b = data[i] & 0xFF;
//
//            // 2. Swap Nibbles:
//            //    (b >>> 4) moves high nibble to low (unsigned shift)
//            //    (b << 4)  moves low nibble to high
//            int swapped = (b >>> 4) | (b << 4);
//
//            // 3. XOR with 0xA5 and cast back to byte
//            data[i] = (byte) (swapped ^ 0xA5);
//        }
//        return data;
//    }
    /**
     * Scrambles (obfuscates) a password byte array for use in TDS LOGIN7 packet.
     * This is the exact algorithm specified in [MS-TDS] for SQL Server Authentication.
     *
     * @param data The original password encoded as UTF-16LE bytes
     * @return The scrambled (obfuscated) byte array to send over the wire
     */
    private byte[] scramblePassword(byte[] data) {
        if (data == null || data.length == 0) {
            return new byte[0];
        }

        // The length must be even (UTF-16LE characters)
        if (data.length % 2 != 0) {
            throw new IllegalArgumentException("Password data must be even length (UTF-16LE)");
        }

        byte[] result = new byte[data.length];

        for (int i = 0; i < data.length; i++) {
            int original = data[i] & 0xFF;

            // Step 1: Rotate left by 4 bits (swap high and low nibble)
            int rotated = ((original << 4) & 0xF0) | ((original >>> 4) & 0x0F);

            // Step 2: XOR with 0xA5
            result[i] = (byte) (rotated ^ 0xA5);
        }

        return result;
    }

    @Override
    public String toString() {
        return "Login7Payload[options=" + options + ", username=" + username + "]";
    }
}