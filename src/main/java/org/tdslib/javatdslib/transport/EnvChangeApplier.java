package org.tdslib.javatdslib.transport;

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
 * Handles applying ENVCHANGE tokens to the ConnectionContext.
 * Extracted from TdsTransport to satisfy SRP.
 */
public class EnvChangeApplier {
  private static final Logger logger = LoggerFactory.getLogger(EnvChangeApplier.class);

  private static final Map<Integer, String> COMMON_SORTID_NAMES = new HashMap<>();
  static {
    COMMON_SORTID_NAMES.put(52, "SQL_Latin1_General_CP1_CI_AS (Sort Order 52)");
    COMMON_SORTID_NAMES.put(51, "SQL_Latin1_General_CP1_CS_AS");
    COMMON_SORTID_NAMES.put(54, "SQL_Latin1_General_CP850_CI_AS");
    COMMON_SORTID_NAMES.put(53, "SQL_Latin1_General_CP1_CI_AI");
    COMMON_SORTID_NAMES.put(55, "SQL_Latin1_General_CP850_CS_AS");
  }

  public static void apply(EnvChangeToken change, ConnectionContext context) {
    EnvChangeType type = change.getChangeType();
    Charset charset = context.isUnicodeEnabled() ? StandardCharsets.UTF_16LE : StandardCharsets.US_ASCII;
    ByteBuffer buf = ByteBuffer.wrap(change.getValueBytes());

    switch (type) {
      case DATABASE:
      case LANGUAGE:
      case CHARSET:
        int dbNewCharLen = buf.get() & 0xFF;
        byte[] dbNewData = new byte[dbNewCharLen * 2];
        buf.get(dbNewData);
        String dbNewValue = new String(dbNewData, charset).trim();

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
          }
        } catch (NumberFormatException e) {
          logger.warn("Failed to parse packet size: '{}'", sizeStr);
        }
        break;

      case SQL_COLLATION:
        int newInfoLen = buf.get() & 0xFF;
        byte[] newCollationData = new byte[newInfoLen];
        buf.get(newCollationData);

        context.setCollationBytes(newCollationData);

        if (newInfoLen >= 5) {
          ByteBuffer collationBuf = ByteBuffer.wrap(newCollationData).order(ByteOrder.LITTLE_ENDIAN);
          final int fullLcid = collationBuf.getInt();
          final int baseLcid = fullLcid & 0x000FFFFF;

          logger.info("SQL Collation updated: LCID={}", baseLcid);
        }
        break;

      case RESET_CONNECTION:
      case RESET_CONNECTION_SKIP_TRAN:
        context.resetToDefaults();
        logger.info("Connection reset by server ({})", type);
        break;

      case BEGIN_TRANSACTION:
        context.setInTransaction(true);
        break;
      case COMMIT_TRANSACTION:
      case ROLLBACK_TRANSACTION:
        context.setInTransaction(false);
        break;

      default:
        logger.debug("Unhandled ENVCHANGE type: {}", type);
        break;
    }
  }
}