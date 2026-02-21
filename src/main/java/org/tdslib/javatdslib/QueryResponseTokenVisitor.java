package org.tdslib.javatdslib;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.info.InfoToken;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueToken;
import org.tdslib.javatdslib.tokens.row.RowToken;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QueryResponseTokenVisitor implements Publisher<Result.Segment>, TokenVisitor, Subscription {
  private static final Logger logger = LoggerFactory.getLogger(QueryResponseTokenVisitor.class);
  private static final String SERVER_MESSAGE = "Server message [{}] (state {}): {}";

  private final TdsTransport transport;
  private final TdsMessage queryTdsMessage;
  private final TokenDispatcher tokenDispatcher;

  private Subscriber<? super Result.Segment> subscriber;
  private final AtomicBoolean isQuerySent = new AtomicBoolean(false);
  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicBoolean upstreamDone = new AtomicBoolean(false);

  private final Queue<Result.Segment> buffer = new ConcurrentLinkedQueue<>();

  public QueryResponseTokenVisitor(TdsTransport transport, TdsMessage queryTdsMessage) {
    this.transport = transport;
    this.queryTdsMessage = queryTdsMessage;
    this.tokenDispatcher = new TokenDispatcher();
    this.transport.setClientHandlers(this::messageHandler, this::errorHandler);
  }

  @Override
  public void subscribe(Subscriber<? super Result.Segment> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(this);
  }

  @Override
  public void request(long n) {
    if (n <= 0) {
      subscriber.onError(new IllegalArgumentException("Request must be > 0"));
      return;
    }
    addDemand(n);
    if (isQuerySent.compareAndSet(false, true)) {
      try {
        transport.sendQueryMessageAsync(queryTdsMessage);
      } catch (IOException e) {
        subscriber.onError(e);
      }
    } else {
      drain();
    }
  }

  @Override
  public void cancel() {
    isCancelled.set(true);
    transport.cancelCurrent();
  }

  private void messageHandler(TdsMessage tdsMessage) {
    if (isCancelled.get()) return;
    tokenDispatcher.processMessage(tdsMessage, transport, new QueryContext(), this);
  }

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    if (isCancelled.get()) return;
    Result.Segment segment = null;

    switch (token.getType()) {
      case ROW:
        segment = createRowSegment((RowToken) token, queryContext);
        break;
      case DONE:
      case DONE_IN_PROC:
      case DONE_PROC:
        DoneToken done = (DoneToken) token;
        if (!queryContext.getReturnValues().isEmpty()) {
          segment = createOutSegment(queryContext);
          queryContext.getReturnValues().clear();
        } else if (done.getStatus().hasCount()) {
          segment = new TdsUpdateCount(done.getCount());
        }
        if (!done.getStatus().hasMoreResults() && !queryContext.isHasError()) {
          upstreamDone.set(true);
        }
        break;
      case RETURN_VALUE:
        queryContext.addReturnValue((ReturnValueToken) token);
        break;
      case INFO:
        InfoToken info = (InfoToken) token;
        logger.info(SERVER_MESSAGE, info.getNumber(), info.getState(), info.getMessage());
        if (info.isError()) queryContext.setHasError(true);
        break;
      case ERROR:
        ErrorToken error = (ErrorToken) token;
        logger.error(SERVER_MESSAGE, error.getNumber(), error.getState(), error.getMessage());
        if (error.isError()) {
          R2dbcException exception = new R2dbcNonTransientResourceException(error.getMessage(), String.valueOf(error.getState()), (int) error.getNumber());
          subscriber.onError(exception);
          queryContext.setHasError(true);
        }
        break;
      case ENV_CHANGE:
        transport.applyEnvChange((EnvChangeToken) token);
        break;
    }

    if (segment != null) {
      buffer.offer(segment);
    }
    drain();
  }

  private void drain() {
    // FIX: Fail loudly if data arrives without a subscriber
    if (subscriber == null) {
      if (!buffer.isEmpty()) {
        // 1. Mark as cancelled to stop further processing
        isCancelled.set(true);

        // 2. Attempt to shut down the transport
        try {
          transport.cancelCurrent();
        } catch (Exception e) {
          // Log but don't suppress the main error
          logger.error("Failed to cancel transport during illegal state shutdown", e);
        }

        // 3. Throw exception to the caller (Transport Thread)
        throw new IllegalStateException("Protocol Violation: Received " + buffer.size() + " segments without a Subscriber. " +
            "This implies data arrived before 'subscribe()' or after 'cancel()'.");
      }
      return; // Safe to ignore spurious calls if buffer is empty
    }

    // Normal drain loop...
    while (!buffer.isEmpty() && demand.get() > 0) {
      Result.Segment s = buffer.poll();
      if (s != null) {
        subscriber.onNext(s);
        demand.decrementAndGet();
      }
    }

    if (upstreamDone.get() && buffer.isEmpty()) {
      if (isCancelled.compareAndSet(false, true)) {
        subscriber.onComplete();
      }
    }
  }

  private void addDemand(long n) {
    long current, next;
    do {
      current = demand.get();
      next = current + n;
      if (next < 0) next = Long.MAX_VALUE;
    } while (!demand.compareAndSet(current, next));
  }

  private Result.Segment createRowSegment(RowToken token, QueryContext ctx) {
    List<ColumnMetadata> metaList = new ArrayList<>();
    if (ctx.getColMetaDataToken() != null) {
      for (ColumnMeta cm : ctx.getColMetaDataToken().getColumns()) {
        metaList.add(new TdsColumnMetadata(cm));
      }
    }
    return new TdsRowSegment(new TdsRow(token.getColumnData(), metaList));
  }

  private Result.OutSegment createOutSegment(QueryContext ctx) {
    List<ReturnValueToken> tokens = ctx.getReturnValues();
    List<byte[]> values = new ArrayList<>(tokens.size());
    List<TdsOutParameterMetadata> metaList = new ArrayList<>(tokens.size());

    for (ReturnValueToken token : tokens) {
      // Cast the Object return to byte[] since TDS delivers raw buffers here
      values.add((byte[]) token.getValue());
      metaList.add(new TdsOutParameterMetadata(token));
    }

    return new TdsOutSegment(new TdsOutParameters(values, metaList));
  }

  private void errorHandler(Throwable t) {
    if (subscriber != null && !isCancelled.get()) subscriber.onError(t);
  }
}