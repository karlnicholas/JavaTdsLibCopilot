// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.payloads.login7;

import com.microsoft.data.tools.tdslib.payloads.Payload;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import com.microsoft.data.tools.tdslib.buffer.ByteBufferUtil;
import com.microsoft.data.tools.tdslib.payloads.login7.auth.FedAuth;

public final class Login7Payload extends Payload {
    private static final byte FeatureExtensionTerminator = (byte) 0xFF;
    private static final int ClientIdSize = 6;

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

        buildBufferInternal();
    }

    @Override
    protected void buildBufferInternal() {
        // Implements TDS Login7 payload serialization.
        // See: https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/ 
        // This implementation covers the main fields and offsets.

        // Collect variable-length fields
        String[] varFields = new String[] {
            hostname != null ? hostname : "",
            username != null ? username : "",
            password != null ? password : "",
            appName != null ? appName : "",
            serverName != null ? serverName : "",
            libraryName != null ? libraryName : "",
            language != null ? language : "",
            database != null ? database : ""
        };

        // Password scrambling (TDS style)
        byte[] scrambledPassword = password != null ? scramblePassword(password.getBytes(java.nio.charset.StandardCharsets.UTF_16LE)) : new byte[0];

        // Calculate offsets (header is 94 bytes)
        int headerSize = 94;
        int[] offsets = new int[varFields.length];
        int[] lengths = new int[varFields.length];
        int curOffset = headerSize;
        for (int i = 0; i < varFields.length; i++) {
            byte[] bytes = (i == 2) ? scrambledPassword : varFields[i].getBytes(java.nio.charset.StandardCharsets.UTF_16LE);
            lengths[i] = bytes.length / 2; // length in WCHARs
            offsets[i] = bytes.length > 0 ? curOffset : 0;
            curOffset += bytes.length;
        }

        // AttachDbFile, ChangePassword, SSPI, Extensions
        byte[] attachDbFileBytes = attachDbFile != null ? attachDbFile.getBytes(java.nio.charset.StandardCharsets.UTF_16LE) : new byte[0];
        int attachDbFileOffset = attachDbFileBytes.length > 0 ? curOffset : 0;
        int attachDbFileLen = attachDbFileBytes.length / 2;
        curOffset += attachDbFileBytes.length;

        byte[] changePasswordBytes = changePassword != null ? changePassword.getBytes(java.nio.charset.StandardCharsets.UTF_16LE) : new byte[0];
        int changePasswordOffset = changePasswordBytes.length > 0 ? curOffset : 0;
        int changePasswordLen = changePasswordBytes.length / 2;
        curOffset += changePasswordBytes.length;

        byte[] sspiBytes = sspi != null ? new byte[sspi.remaining()] : new byte[0];
        if (sspi != null) {
            sspi.slice().get(sspiBytes);
        }
        int sspiOffset = sspiBytes.length > 0 ? curOffset : 0;
        int sspiLen = sspiBytes.length;
        curOffset += sspiBytes.length;

        ByteBuffer extensionsBuffer = getExtensionsBuffer();
        byte[] extensionsBytes = new byte[extensionsBuffer.remaining()];
        extensionsBuffer.slice().get(extensionsBytes);
        int extensionsOffset = extensionsBytes.length > 0 ? curOffset : 0;
        int extensionsLen = extensionsBytes.length;
        curOffset += extensionsBytes.length;

        // Allocate buffer
        buffer = java.nio.ByteBuffer.allocate(curOffset).order(java.nio.ByteOrder.LITTLE_ENDIAN);

        // Write header fields
        buffer.putInt(options.getTdsVersion().getValue()); // TDS version
        buffer.putInt(options.getPacketSize()); // Packet size
        buffer.putInt((int) options.getClientProgVer()); // ClientProgVer
        buffer.putInt((int) options.getClientPid()); // ClientPID
        buffer.putInt((int) options.getConnectionId()); // ConnectionID
        buffer.put(optionFlags1 != null ? optionFlags1.toByte() : 0); // OptionFlags1
        buffer.put(optionFlags2 != null ? optionFlags2.toByte() : 0); // OptionFlags2
        buffer.put(optionFlags3 != null ? optionFlags3.toByte() : 0); // OptionFlags3
        buffer.put(typeFlags != null ? typeFlags.toByte() : 0); // TypeFlags
        buffer.putShort((short) 0); // Reserved (2 bytes)

        // Offsets and lengths for variable fields (in WCHARs, except SSPI/Extensions)
        for (int i = 0; i < varFields.length; i++) {
            buffer.putShort((short) offsets[i]);
            buffer.putShort((short) lengths[i]);
        }
        buffer.putShort((short) attachDbFileOffset);
        buffer.putShort((short) attachDbFileLen);
        buffer.putShort((short) changePasswordOffset);
        buffer.putShort((short) changePasswordLen);
        buffer.putShort((short) sspiOffset);
        buffer.putShort((short) sspiLen);
        buffer.putShort((short) extensionsOffset);
        buffer.putShort((short) extensionsLen);

        buffer.putInt(options.getClientTimeZone());
        buffer.putInt((int) options.getClientLcid());

        // Write variable-length fields in order
        for (int i = 0; i < varFields.length; i++) {
            byte[] bytes = (i == 2) ? scrambledPassword : varFields[i].getBytes(java.nio.charset.StandardCharsets.UTF_16LE);
            buffer.put(bytes);
        }
        buffer.put(attachDbFileBytes);
        buffer.put(changePasswordBytes);
        buffer.put(sspiBytes);
        buffer.put(extensionsBytes);
        buffer.flip();
    }

    private byte[] scramblePassword(byte[] data) {
        for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            b = (byte) ((b >> 4) | (b << 4));
            b ^= (byte) 0xA5;
            data[i] = b;
        }
        return data;
    }

    private byte[] generateRandomPhysicalAddress() {
        byte[] addr = new byte[ClientIdSize];
        new Random().nextBytes(addr);
        return addr;
    }

    private ByteBuffer getExtensionsBuffer() {
        List<ByteBuffer> buffers = new ArrayList<>();
        if (fedAuth != null) {
            ByteBuffer b = fedAuth.getBuffer();
            if (b != null) buffers.add(b);
        }
        buffers.add(ByteBuffer.wrap(new byte[] { FeatureExtensionTerminator }));

        // concatenate into single buffer
        int len = 0;
        for (ByteBuffer b : buffers) len += b.remaining();
        ByteBuffer out = ByteBuffer.allocate(len);
        for (ByteBuffer b : buffers) out.put(b.slice());
        out.flip();
        return out;
    }

    @Override
    public String toString() {
        return "Login7Payload[options=" + options + ", username=" + username + "]";
    }
}
