package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.packets.OutboundTdsMessage;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * High-level TDS client facade.
 * Provides a simple connect + execute interface, hiding protocol details.
 */
public class TdsConnection implements Connection {
  private final TdsTransport transport;
  private final ConnectionContext context;

  // --- Transaction Manager Operation Codes ---
  private static final short TM_BEGIN_XACT = 5;
  private static final short TM_COMMIT_XACT = 7;
  private static final short TM_ROLLBACK_XACT = 8;

  // --- Transaction Flags & Lengths ---
  private static final byte ISOLATION_LEVEL_DEFAULT = 0x00;
  private static final byte TX_NAME_LENGTH_EMPTY = 0x00;
  private static final byte TM_FLAG_DEFAULT = 0x00;

  // --- SQL Server Isolation Level IDs ---
  private static final byte ISOLATION_READ_UNCOMMITTED = 0x01;
  private static final byte ISOLATION_READ_COMMITTED = 0x02;
  private static final byte ISOLATION_REPEATABLE_READ = 0x03;
  private static final byte ISOLATION_SERIALIZABLE = 0x04;

  /**
   * Create a new TdsConnection backed by a TCP transport to the given host/port.
   *
   * @param transport TdsTransport
   * @param context   The connection state context
   */
  public TdsConnection(TdsTransport transport, ConnectionContext context) {
    this.transport = transport;
    this.context = context;
  }

  @Override
  public Publisher<Void> beginTransaction() {
    return Mono.from(transport.execute(headers -> {
      ByteBuffer payload = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      payload.putShort(TM_BEGIN_XACT);
      payload.put(ISOLATION_LEVEL_DEFAULT);
      payload.put(TX_NAME_LENGTH_EMPTY);
      payload.flip();

      return OutboundTdsMessage.createWithHeaders(PacketType.TRANSACTION_MANAGER, headers, Mono.just(payload));
    })).then();
  }

  @Override
  public Publisher<Void> beginTransaction(TransactionDefinition definition) {
    return Mono.from(transport.execute(headers -> {
      IsolationLevel level = definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL);
      String txName = definition.getAttribute(TransactionDefinition.NAME);

      txName = compressTransactionName(txName);
      byte tdsIsolationLevel = mapIsolationLevel(level);

      byte[] nameBytes = (txName != null && !txName.isEmpty())
          ? txName.getBytes(StandardCharsets.UTF_16LE)
          : new byte[0];

      int nameLenBytes = nameBytes.length;

      ByteBuffer payload = ByteBuffer.allocate(4 + nameBytes.length).order(ByteOrder.LITTLE_ENDIAN);
      payload.putShort(TM_BEGIN_XACT);
      payload.put(tdsIsolationLevel);
      payload.put((byte) nameLenBytes);
      if (nameLenBytes > 0) {
        payload.put(nameBytes);
      }
      payload.flip();

      return OutboundTdsMessage.createWithHeaders(PacketType.TRANSACTION_MANAGER, headers, Mono.just(payload));
    })).then();
  }

  /**
   * Compresses a fully qualified method name to fit within SQL Server's 32-character
   * transaction name limit, mimicking SLF4J logger name compression.
   * Example: "org.example.service.CourseService.getAllCourses"
   * -> "o.e.s.CourseService.getAllCourses"
   */
  private String compressTransactionName(String txName) {
    if (txName == null || txName.length() <= 32) {
      return txName;
    }

    String[] parts = txName.split("\\.");

    // We only want to compress the package names, leaving the Class and Method
    // (the last two parts) intact if possible.
    int packagePartsToCompress = parts.length - 2;

    for (int i = 0; i < packagePartsToCompress; i++) {
      if (parts[i].length() > 1) {
        // Compress this package part to just its first letter
        parts[i] = parts[i].substring(0, 1);
      }

      // Re-calculate the current total length
      int currentLen = parts.length - 1; // Account for the dots
      for (String p : parts) {
        currentLen += p.length();
      }

      // Stop compressing if we've successfully shrunk it under the limit
      if (currentLen <= 32) {
        break;
      }
    }

    String compressed = String.join(".", parts);

    // If it is STILL over 32 characters (e.g., very long class or method name),
    // apply a hard truncation. We take the LAST 32 characters because the end
    // of the string contains the method name, which is the most critical
    // part for debugging active sessions in SQL Server.
    if (compressed.length() > 32) {
      return compressed.substring(compressed.length() - 32);
    }

    return compressed;
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return Mono.defer(() -> {
      if (!context.isInTransaction()) {
        return Mono.empty();
      }

      return Mono.from(transport.execute(headers -> {
        ByteBuffer payload = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        payload.putShort(TM_COMMIT_XACT);
        payload.put(TM_FLAG_DEFAULT);
        payload.put(TX_NAME_LENGTH_EMPTY);
        payload.flip();

        return OutboundTdsMessage.createWithHeaders(PacketType.TRANSACTION_MANAGER, headers, Mono.just(payload));
      })).then();
    });
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    return Mono.defer(() -> {
      if (!context.isInTransaction()) {
        return Mono.empty();
      }

      return Mono.from(transport.execute(headers -> {
        ByteBuffer payload = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        payload.putShort(TM_ROLLBACK_XACT);
        payload.put(TM_FLAG_DEFAULT);
        payload.put(TX_NAME_LENGTH_EMPTY);
        payload.flip();

        return OutboundTdsMessage.createWithHeaders(PacketType.TRANSACTION_MANAGER, headers, Mono.just(payload));
      })).then();
    });
  }

  private byte mapIsolationLevel(IsolationLevel level) {
    if (level == null) {
      return ISOLATION_LEVEL_DEFAULT;
    }
    if (level == IsolationLevel.READ_UNCOMMITTED) {
      return ISOLATION_READ_UNCOMMITTED;
    }
    if (level == IsolationLevel.READ_COMMITTED) {
      return ISOLATION_READ_COMMITTED;
    }
    if (level == IsolationLevel.REPEATABLE_READ) {
      return ISOLATION_REPEATABLE_READ;
    }
    if (level == IsolationLevel.SERIALIZABLE) {
      return ISOLATION_SERIALIZABLE;
    }
    return ISOLATION_LEVEL_DEFAULT; // Default fallback
  }

  @Override
  public Publisher<Void> close() {
    return Mono.fromRunnable(() -> {
      try {
        transport.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close TDS transport", e);
      }
    });
  }

  @Override
  public Batch createBatch() {
    return new TdsBatch(this.transport, this.context);
  }

  @Override
  public Statement createStatement(String sql) {
    return new TdsStatement(this.transport, context, sql);
  }

  // --- Unimplemented / Stub Methods below ---

  @Override
  public Publisher<Void> createSavepoint(String name) {
    return Mono.error(new UnsupportedOperationException("Savepoints are not yet supported"));
  }

  @Override
  public boolean isAutoCommit() {
    return !context.isInTransaction();
  }

  @Override
  public ConnectionMetadata getMetadata() {
    return null;
  }

  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    return null; // Can be derived if you store it in ConnectionContext
  }

  @Override
  public Publisher<Void> releaseSavepoint(String name) {
    return Mono.error(new UnsupportedOperationException("Savepoints are not yet supported"));
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String name) {
    return Mono.error(new UnsupportedOperationException("Savepoints are not yet supported"));
  }

  @Override
  public Publisher<Void> setAutoCommit(boolean autoCommit) {
    return Mono.error(new UnsupportedOperationException(
        "setAutoCommit is not supported directly via R2DBC SPI"));
  }

  @Override
  public Publisher<Void> setLockWaitTimeout(Duration timeout) {
    return Mono.empty();
  }

  @Override
  public Publisher<Void> setStatementTimeout(Duration timeout) {
    return Mono.empty();
  }

  @Override
  public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    // Optional: You can implement this by sending a 0x0E with Type 5 (Change Isolation Level)
    return Mono.empty();
  }

  @Override
  public Publisher<Boolean> validate(ValidationDepth depth) {
    return Mono.just(true); // Simplified stub
  }

  public TdsTransport getTransport() {
    return  transport;
  }
}