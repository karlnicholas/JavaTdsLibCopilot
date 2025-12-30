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
        // Minimal implementation: build an empty payload buffer.
        // A fuller implementation would follow the original C# assembly of parts.
        buffer = ByteBuffer.allocate(0);
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
