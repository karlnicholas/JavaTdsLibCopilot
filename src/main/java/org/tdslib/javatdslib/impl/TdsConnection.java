package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
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
    // Change to accept the headers from the Transport
    return Mono.from(transport.execute(headers -> {
      ByteBuffer payload = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      payload.putShort((short) 5); // TM_BEGIN_XACT
      payload.put((byte) 0x00);    // Isolation Level: 0x00 (Use session default)
      payload.put((byte) 0x00);    // Transaction Name Length: 0 (Unnamed)
      payload.flip();

      // Transport automatically provides the correct headers!
      return TdsMessage.createWithHeaders(PacketType.TRANSACTION_MANAGER, headers, payload);
    })).then();
  }

  @Override
  public Publisher<Void> beginTransaction(TransactionDefinition definition) {
    // Change to accept the headers from the Transport
    return Mono.from(transport.execute(headers -> {
      IsolationLevel level = definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL);
      String txName = definition.getAttribute(TransactionDefinition.NAME);

      byte tdsIsolationLevel = mapIsolationLevel(level);

      byte[] nameBytes = (txName != null && !txName.isEmpty())
          ? txName.getBytes(StandardCharsets.UTF_16LE)
          : new byte[0];

      int nameLenChars = nameBytes.length / 2;

      ByteBuffer payload = ByteBuffer.allocate(5 + nameBytes.length).order(ByteOrder.LITTLE_ENDIAN);
      payload.putShort((short) 5);            // TM_BEGIN_XACT
      payload.put(tdsIsolationLevel);
      payload.put((byte) 0x00);
      payload.put((byte) nameLenChars);
      if (nameLenChars > 0) {
        payload.put(nameBytes);
      }
      payload.flip();

      return TdsMessage.createWithHeaders(PacketType.TRANSACTION_MANAGER, headers, payload);
    })).then();
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return Mono.defer(() -> {
      if (!context.isInTransaction()) {
        return Mono.empty();
      }

      // Change to accept the headers from the Transport
      return Mono.from(transport.execute(headers -> {
        ByteBuffer payload = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        payload.putShort((short) 7); // TM_COMMIT_XACT
        payload.put((byte) 0x00);
        payload.put((byte) 0x00);
        payload.flip();

        return TdsMessage.createWithHeaders(PacketType.TRANSACTION_MANAGER, headers, payload);
      })).then();
    });
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    return Mono.defer(() -> {
      if (!context.isInTransaction()) {
        return Mono.empty();
      }

      // Change to accept the headers from the Transport
      return Mono.from(transport.execute(headers -> {
        ByteBuffer payload = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        payload.putShort((short) 8); // TM_ROLLBACK_XACT
        payload.put((byte) 0x00);
        payload.put((byte) 0x00);
        payload.flip();

        return TdsMessage.createWithHeaders(PacketType.TRANSACTION_MANAGER, headers, payload);
      })).then();
    });
  }

  private byte mapIsolationLevel(IsolationLevel level) {
    if (level == null) {
      return 0x00;
    }
    if (level == IsolationLevel.READ_UNCOMMITTED) {
      return 0x01;
    }
    if (level == IsolationLevel.READ_COMMITTED) {
      return 0x02;
    }
    if (level == IsolationLevel.REPEATABLE_READ) {
      return 0x03;
    }
    if (level == IsolationLevel.SERIALIZABLE) {
      return 0x04;
    }
    return 0x00; // Default fallback
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
}