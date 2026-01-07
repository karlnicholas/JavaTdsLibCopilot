package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.ApplyingTokenVisitor;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeType;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.info.InfoToken;
import org.tdslib.javatdslib.tokens.loginack.LoginAckToken;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the result of a Login7 request.
 * Collects success/failure status and any side-effects (environment changes, errors).
 */
public class LoginResponse implements TokenVisitor {

    private boolean success = false;
    private String errorMessage = null;
    private String database = null;

    private final List<EnvChangeToken> envChanges = new ArrayList<>();

    // --- Mutators (used during token processing) ---
    private static final Logger logger = LoggerFactory.getLogger(ApplyingTokenVisitor.class);

//    private final ConnectionContext context;

//    public ApplyingTokenVisitor(ConnectionContext context) {
//        this.context = context;
//    }

    @Override
    public void onToken(Token token) {
        if (token instanceof EnvChangeToken envChange) {
            applyEnvChange(envChange);
        } else if (token instanceof LoginAckToken ack) {
            context.setTdsVersion(ack.getTdsVersion());
            context.setServerName(ack.getServerName());
            context.setServerVersionString(ack.getServerVersionString());
            logger.info("Login successful - TDS version: {}, Server name: {}, Server version: {}", ack.getTdsVersion(), ack.getServerName(), ack.getServerVersionString());
        } else if (token instanceof ErrorToken err) {
            logger.warn("Server error [{}]: {}", err.getNumber(), err.getMessage());
        } else if (token instanceof InfoToken info) {
            // Severity 0–10 = info, >10 = error (but INFO token is always <=10)
            logger.info("Server info [{}] (state {}): {}", info.getNumber(), info.getState(), info.getMessage());
        } else if (token instanceof DoneToken done) {
            if (done.hasError()) {
                logger.warn("Batch completed with error (status: {})", done.getStatus());
            } else {
                logger.debug("Batch completed successfully (status: {})", done.getStatus());
            }
        } else {
            logger.debug("Unhandled token type: {}", token.getType());
        }
    }

    /**
     * Applies an ENVCHANGE token to the connection context.
     * Decodes values correctly based on EnvChangeType (char count vs bytes, UTF-16LE vs ASCII).
     */
    private void applyEnvChange(EnvChangeToken change) {
        EnvChangeType type = change.getChangeType();

        // Determine charset for string-based changes (TDS 7.0+ uses UTF-16LE)
        Charset charset = context.isUnicodeEnabled()
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
                    context.setDatabase(dbNewValue);
                    logger.info("Database changed from '{}' to '{}'", dbOldValue, dbNewValue);
                } else if (type == EnvChangeType.LANGUAGE) {
                    context.setLanguage(dbNewValue);
                    logger.info("Language changed from '{}' to '{}'", dbOldValue, dbNewValue);
                } else if (type == EnvChangeType.CHARSET) {
                    context.setCharset(dbNewValue);
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
                        context.setPacketSize(newSize);
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
                context.setCollationBytes(newCollationData);

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
                context.resetToDefaults();
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

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        this.success = false; // error implies failure
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void addEnvChange(EnvChangeToken change) {
        if (change != null) {
            envChanges.add(change);
        }
    }

    // --- Accessors ---

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getDatabase() {
        return database;
    }

    /**
     * Returns an unmodifiable view of the collected environment changes.
     */
    public List<EnvChangeToken> getEnvChanges() {
        return Collections.unmodifiableList(envChanges);
    }

    /**
     * Convenience: returns true if any ENVCHANGE tokens were received.
     */
    public boolean hasEnvChanges() {
        return !envChanges.isEmpty();
    }

    @Override
    public String toString() {
        return "LoginResponse{" +
                "success=" + success +
                ", errorMessage='" + errorMessage + '\'' +
                ", database='" + database + '\'' +
                ", envChanges=" + envChanges.size() + " item(s)" +
                '}';
    }
}