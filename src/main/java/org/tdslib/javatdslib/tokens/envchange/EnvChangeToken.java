package org.tdslib.javatdslib.tokens.envchange;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Represents an ENVCHANGE token (0xE3) in the TDS protocol.
 *
 * This token notifies the client of environment changes such as database, packet size,
 * language, collation, transaction state, etc.
 *
 * The values are stored as raw bytes to support both ASCII and UCS-2 Unicode encoding.
 * String decoding should be handled by the TokenVisitor or ConnectionContext based
 * on the connection's Unicode mode (TDS version ≥ 7.0).
 */
public class EnvChangeToken extends Token {

    private final EnvChangeType changeType;
    private final byte[] valueBytes;  // All data after the change type byte

    public EnvChangeToken(byte type, EnvChangeType changeType, byte[] valueBytes) {
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
        return "EnvChangeToken{type=" + changeType + ", valueBytes=" + bytesToHex(valueBytes) + "}";
    }

    private static String bytesToHex(byte[] bytes) {
        if (bytes.length == 0) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (byte b : bytes) sb.append(String.format("%02X ", b));
        sb.setLength(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }
}