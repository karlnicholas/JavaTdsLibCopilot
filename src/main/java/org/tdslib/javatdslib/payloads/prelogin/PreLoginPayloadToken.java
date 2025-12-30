package org.tdslib.javatdslib.payloads.prelogin;

/**
 * Represents a PreLoginPayloadToken, used in the TDS pre-login handshake.
 * This is a placeholder for the structure as defined in the original C# implementation.
 */
public class PreLoginPayloadToken {
    private final PreLoginOption option;
    private final int offset;
    private final int length;

    public PreLoginPayloadToken(PreLoginOption option, int offset, int length) {
        this.option = option;
        this.offset = offset;
        this.length = length;
    }

    public PreLoginOption getOption() {
        return option;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }
}
