package org.tdslib.javatdslib.payloads.login7.auth;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class SecurityTokenFedAuth extends FedAuth {
    private final String token;
    private final boolean echo;

    public SecurityTokenFedAuth(String token, boolean echo) {
        if (token == null || token.isEmpty()) throw new IllegalArgumentException("token");
        this.token = token;
        this.echo = echo;
    }

    @Override
    public ByteBuffer getBuffer() {
        byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_16LE);
        ByteBuffer tokenBuffer = ByteBuffer.wrap(tokenBytes);

        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.put(FeatureId);
        buffer.putInt(tokenBuffer.remaining() + 4 + 1);
        byte options = (byte) (LibrarySecurityToken | (echo ? FedAuthEchoYes : FedAuthEchoNo));
        buffer.put(options);
        buffer.putInt(tokenBuffer.remaining());
        buffer.flip();

        ByteBuffer out = ByteBuffer.allocate(buffer.remaining() + tokenBuffer.remaining());
        out.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        out.put(buffer);
        out.put(tokenBuffer);
        out.flip();
        return out;
    }
}
