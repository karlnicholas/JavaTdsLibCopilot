package org.tdslib.javatdslib;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.time.Duration;

/**
 * High-level TDS client facade.
 * Provides a simple connect + execute interface, hiding protocol details.
 */
public class TdsConnection implements Connection {
  private static final Logger logger = LoggerFactory.getLogger(TdsConnection.class);

  private final TdsTransport transport;
//  private boolean connected;

  /**
   * Create a new TdsConnection backed by a TCP transport to the given host/port.
   *
   * @param transport TdsTransport
   */
  public TdsConnection(TdsTransport transport) {
    this.transport = transport;
//    this.connected = true;
  }


  @Override
  public Publisher<Void> beginTransaction() {
    return null;
  }

  @Override
  public Publisher<Void> beginTransaction(TransactionDefinition definition) {
    return null;
  }

  @Override
  public Publisher<Void> close() {
    return subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {
          try {
            transport.close();
            subscriber.onComplete();
          } catch (IOException e) {
            subscriber.onError(e);
          }
        }

        @Override
        public void cancel() {
          // No-op
        }
      });
    };
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return null;
  }

  @Override
  public Batch createBatch() {
    return new TdsBatch(this.transport);
  }

  @Override
  public Publisher<Void> createSavepoint(String name) {
    return null;
  }

  /**
   * Execute a SQL query and return the high-level QueryResponse.
   *
   * @param sql SQL text to execute
   * @return QueryResponse containing results or errors
   * @throws IOException on I/O or transport errors
   */
  @Override
  public Statement createStatement(String sql) {
    return new TdsStatement(this.transport, sql);
  }

  @Override
  public boolean isAutoCommit() {
    return false;
  }

  @Override
  public ConnectionMetadata getMetadata() {
    return null;
  }

  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    return null;
  }

  @Override
  public Publisher<Void> releaseSavepoint(String name) {
    return null;
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    return null;
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String name) {
    return null;
  }

  @Override
  public Publisher<Void> setAutoCommit(boolean autoCommit) {
    return null;
  }

  @Override
  public Publisher<Void> setLockWaitTimeout(Duration timeout) {
    return null;
  }

  @Override
  public Publisher<Void> setStatementTimeout(Duration timeout) {
    return null;
  }

  @Override
  public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    return null;
  }

  @Override
  public Publisher<Boolean> validate(ValidationDepth depth) {
    return null;
  }

//  public boolean isConnected() {
//    return connected;
//  }

  public SocketChannel getSocketChannel() {
    return transport.getSocketChannel();
  }

}
