package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.protocol.TdsServerErrorException;
import org.tdslib.javatdslib.reactive.R2dbcErrorTranslator;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of the R2DBC {@link Batch} interface for executing multiple SQL statements as a
 * single unit. This class buffers the SQL statements and sends them to the server in a single
 * request when executed.
 */
public class TdsBatch implements Batch {

  private final TdsTransport transport;
  private final ConnectionContext context;
  private final List<String> statements = new ArrayList<>();

  /**
   * Constructs a new TdsBatch.
   *
   * @param transport The transport layer for sending the batch execution request.
   * @param context   The connection context associated with this batch.
   */
  public TdsBatch(TdsTransport transport, ConnectionContext context) {
    this.transport = transport;
    this.context = context;
  }

  @Override
  public Batch add(String sql) {
    if (sql == null) {
      throw new IllegalArgumentException("SQL cannot be null");
    }
    statements.add(sql);
    return this;
  }

  @Override
  public Publisher<? extends Result> execute() {
    if (statements.isEmpty()) {
      return Flux.empty();
    }

    String batchSql = String.join(";\n", statements);
    TdsMessage message = createSqlBatchMessage(batchSql);

    // TdsBatch delegates entirely to the transport execution engine
    return transport.execute(message)
        .windowUntil(this::isBoundarySegment)
        .map(TdsResult::new)
        .onErrorMap(TdsServerErrorException.class, R2dbcErrorTranslator::translateException);
  }

  /**
   * Helper method for Project Reactor's windowUntil operator.
   * Determines if a segment marks the end of a specific SQL statement execution.
   */
  private boolean isBoundarySegment(Result.Segment segment) {
    return segment instanceof org.tdslib.javatdslib.internal.TdsUpdateCount
        || segment instanceof Result.OutSegment;
  }

  private TdsMessage createSqlBatchMessage(String sql) {
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
    byte[] headers = AllHeaders.forAutoCommit(1).toBytes();
    ByteBuffer payload = ByteBuffer.allocate(headers.length + sqlBytes.length);
    payload.put(headers);
    payload.put(sqlBytes);
    payload.flip();
    return TdsMessage.createRequest(PacketType.SQL_BATCH.getValue(), payload);
  }
}