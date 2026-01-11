package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Visitor that applies ENVCHANGE tokens to a {@link ConnectionContext}.
 *
 * <p>Decodes token payloads correctly (char counts vs bytes, UTF-16LE vs ASCII)
 * and updates the provided {@code ConnectionContext}.
 */
public class EnvChangeTokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(EnvChangeTokenVisitor.class);
  private final ConnectionContext connectionContext;

  private static final Map<Integer, String> COMMON_SORTID_NAMES = new HashMap<>();

  static {
    COMMON_SORTID_NAMES.put(52, "SQL_Latin1_General_CP1_CI_AS (Sort Order 52)");
    COMMON_SORTID_NAMES.put(51, "SQL_Latin1_General_CP1_CS_AS");
    COMMON_SORTID_NAMES.put(54, "SQL_Latin1_General_CP850_CI_AS");
    COMMON_SORTID_NAMES.put(53, "SQL_Latin1_General_CP1_CI_AI");
    COMMON_SORTID_NAMES.put(55, "SQL_Latin1_General_CP850_CS_AS");
    // You can add more legacy SQL collations from sys.collations where name LIKE 'SQL_%'
    // Full list: https://learn.microsoft.com/en-us/sql/relational-databases/collations/sql-server-collations
  }

  /**
   * Creates a new visitor that will apply env changes to the given context.
   *
   * @param connectionContext target connection context to update
   */
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

        // Decode and log meaningful collation info
        if (newInfoLen >= 5) {
          ByteBuffer collationBuf = ByteBuffer.wrap(newCollationData)
              .order(ByteOrder.LITTLE_ENDIAN);

          final int fullLcid = collationBuf.getInt(); // bytes 0-3: LCID + flags
          final byte verSortByte = collationBuf.get(); // byte 4: version+sort nibble

          final int flags = (fullLcid >>> 20) & 0xFF; // sensitivity flags

          final int fullSortOrderId = verSortByte & 0xFF;
          final int versionNibble = (fullSortOrderId >>> 4) & 0x0F;
          final int sortNibble = fullSortOrderId & 0x0F;

          final String friendlyName = COMMON_SORTID_NAMES.getOrDefault(
              fullSortOrderId,
              "Unknown legacy SQL collation (sort order " + fullSortOrderId + ")"
          );

          // Build detailed flag description
          StringBuilder flagDesc = new StringBuilder();
          if ((flags & 0x01) != 0) {
            flagDesc.append("CI, ");
          }
          if ((flags & 0x02) != 0) {
            flagDesc.append("AI, ");
          }
          if ((flags & 0x04) != 0) {
            flagDesc.append("WI, ");
          }
          if ((flags & 0x08) != 0) {
            flagDesc.append("KI, ");
          }
          if ((flags & 0x10) != 0) {
            flagDesc.append("BIN, ");
          }
          if ((flags & 0x20) != 0) {
            flagDesc.append("BIN2, ");
          }
          if ((flags & 0x40) != 0) {
            flagDesc.append("UTF8, ");
          }

          final String flagsStr = flagDesc.length() > 0
              ? flagDesc.substring(0, flagDesc.length() - 2)
              : "none";

          final int baseLcid = fullLcid & 0x000FFFFF; // lower 20 bits = locale ID

          // Shortened log message to satisfy LineLength check
          logger.info(
              "SQL Collation updated: {} (LCID={}, flags={}, ver={}, sort={}, id={})",
              friendlyName,
              baseLcid,
              flagsStr,
              versionNibble,
              sortNibble,
              fullSortOrderId
          );
        } else {
          logger.debug("SQL Collation updated (empty or too short: {} bytes)", newInfoLen);
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
