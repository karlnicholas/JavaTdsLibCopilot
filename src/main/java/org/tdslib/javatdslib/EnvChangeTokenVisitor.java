package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class EnvChangeTokenVisitor {
    private static final Logger logger = LoggerFactory.getLogger(EnvChangeTokenVisitor.class);
    private final ConnectionContext connectionContext;

    public EnvChangeTokenVisitor(ConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
    }

    /**
     * Applies an ENVCHANGE token to the connection context.
     * Decodes values correctly based on EnvChangeType (char count vs bytes, UTF-16LE vs ASCII).
     */
    public void applyEnvChange(EnvChangeToken change) {
        EnvChangeType type = change.getChangeType();

        // Determine charset for string-based changes (TDS 7.0+ uses UTF-16LE)
        Charset charset = connectionContext.isUnicodeEnabled()
                ? StandardCharsets.UTF_16LE
                : StandardCharsets.US_ASCII;

        ByteBuffer buf = ByteBuffer.wrap(change.getValueBytes());

        switch (type) {
            case DATABASE:
            case LANGUAGE:
            case CHARSET:
                // New value first (char count + data)
                int dbNewCharLen = buf.get() & 0xFF;
                byte[] dbNewData = new byte[dbNewCharLen * 2];
                buf.get(dbNewData);
                String dbNewValue = new String(dbNewData, charset).trim();

                // Old value second
                int dbOldCharLen = buf.get() & 0xFF;
                byte[] dbOldData = new byte[dbOldCharLen * 2];
                buf.get(dbOldData);
                String dbOldValue = new String(dbOldData, charset).trim();

                if (type == EnvChangeType.DATABASE) {
                    connectionContext.setDatabase(dbNewValue);
                    logger.info("Database changed from '{}' to '{}'", dbOldValue, dbNewValue);
                } else if (type == EnvChangeType.LANGUAGE) {
                    connectionContext.setLanguage(dbNewValue);
                    logger.info("Language changed from '{}' to '{}'", dbOldValue, dbNewValue);
                } else if (type == EnvChangeType.CHARSET) {
                    connectionContext.setCharset(dbNewValue);
                    logger.info("Charset changed from '{}' to '{}'", dbOldValue, dbNewValue);
                }
                break;

            case PACKET_SIZE:
            case PACKET_SIZE_ALT:
                int sizeCharLen = buf.get() & 0xFF;
                byte[] sizeBytes = new byte[sizeCharLen * 2];
                buf.get(sizeBytes);
                String sizeStr = new String(sizeBytes, charset).trim();
                try {
                    int newSize = Integer.parseInt(sizeStr);
                    if (newSize >= 512 && newSize <= 32767) {
                        connectionContext.setPacketSize(newSize);
                        logger.info("Packet size changed to {}", newSize);
                    } else {
                        logger.warn("Invalid packet size: {}", newSize);
                    }
                } catch (NumberFormatException e) {
                    logger.warn("Failed to parse packet size: '{}'", sizeStr);
                }
                break;

            case SQL_COLLATION:
                // New collation
                int newInfoLen = buf.get() & 0xFF;
                byte[] newCollationData = new byte[newInfoLen];
                buf.get(newCollationData);

                // Old collation
                int oldInfoLen = buf.get() & 0xFF;
                byte[] oldCollationData = new byte[oldInfoLen];
                buf.get(oldCollationData);

                // Store the new collation data raw (most clients do this)
                connectionContext.setCollationBytes(newCollationData);

                // Optional: log meaningful info (if you want to decode further)
                if (newInfoLen >= 5) {
                    // Minimal decoding example (LCID is first 4 bytes little-endian)
                    int lcid = ByteBuffer.wrap(newCollationData, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
                    byte versionSortId = newCollationData[4]; // version + sort ID
                    logger.debug("SQL Collation updated: LCID={}, versionSortId={}, length={} bytes", lcid, versionSortId, newInfoLen);
                } else {
                    logger.debug("SQL Collation updated (empty/new length: {})", newInfoLen);
                }
                break;

            case RESET_CONNECTION:
            case RESET_CONNECTION_SKIP_TRAN:
                connectionContext.resetToDefaults();
                logger.info("Connection reset by server ({})", type);
                break;

            case BEGIN_TRANSACTION:
            case COMMIT_TRANSACTION:
            case ROLLBACK_TRANSACTION:
                logger.debug("Transaction state changed: {}", type);
                break;

            case UNKNOWN:
                logger.warn("Received unknown ENVCHANGE type: {}", type);
                break;

            default:
                logger.debug("Unhandled ENVCHANGE type: {}", type);
                break;
        }
    }

}
