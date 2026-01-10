package org.tdslib.javatdslib.tokens.envchange;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Represents an ENVCHANGE token (0xE3) in the TDS protocol.
 *
 * <p>This token notifies the client of environment changes such as database,
 * packet size, language, collation, transaction state, etc.</p>
 *
 * <p>The values are stored as raw bytes to support both ASCII and UCS-2 Unicode
 * encoding. String decoding should be handled by the TokenVisitor or
 * ConnectionContext based on the connection's Unicode mode (TDS version >= 7.0).</p>
 */
public class EnvChangeToken extends Token {

    private final EnvChangeType changeType;
    private final byte[] valueBytes; // All data after the change type byte

    /**
     * Create an EnvChangeToken.
     *
     * @param type       raw token byte value
     * @param changeType parsed change type
     * @param valueBytes raw value bytes (may be empty)
     */
    public EnvChangeToken(final byte type, final EnvChangeType changeType,
            final byte[] valueBytes) {
        super(TokenType.fromValue(type));
        this.changeType = changeType != null ? changeType : EnvChangeType.UNKNOWN;
        this.valueBytes = valueBytes != null ? valueBytes.clone() : new byte[0];
    }

    public EnvChangeType getChangeType() {
        return changeType;
    }

    public byte[] getValueBytes() {
        return valueBytes.clone();
    }

    @Override
    public String toString() {
        return "EnvChangeToken{type=" + changeType
                + ", valueBytes=" + bytesToHex(valueBytes)
                + "}";
    }

    private static String bytesToHex(final byte[] bytes) {
        if (bytes.length == 0) {
            return "[]";
        }
        final StringBuilder sb = new StringBuilder("[");
        for (final byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        sb.setLength(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }
}