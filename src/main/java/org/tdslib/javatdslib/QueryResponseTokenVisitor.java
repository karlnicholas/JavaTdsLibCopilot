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
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.EnvChangeApplier;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class QueryResponseTokenVisitor implements Publisher<Result.Segment>, TokenVisitor, Subscription {
  private static final Logger logger = LoggerFactory.getLogger(QueryResponseTokenVisitor.class);
  private static final String SERVER_MESSAGE = "Server message [{}] (state {}): {}";

  private final TdsTransport transport;
  private final ConnectionContext context;
  private final TdsMessage queryTdsMessage;
  private final TokenDispatcher tokenDispatcher = new TokenDispatcher();
  private final QueryContext queryContext = new QueryContext();

  private Subscriber<? super Result.Segment> subscriber;
  private final AtomicBoolean isQuerySent = new AtomicBoolean(false);
  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicBoolean upstreamDone = new AtomicBoolean(false);
  private final AtomicInteger wip = new AtomicInteger();

  private final Queue<Result.Segment> buffer = new ConcurrentLinkedQueue<>();

  public QueryResponseTokenVisitor(TdsTransport transport, ConnectionContext context, TdsMessage queryTdsMessage) {
    this.transport = transport;
    this.context = context;
    this.queryTdsMessage = queryTdsMessage;
  }

  @Override
  public void subscribe(Subscriber<? super Result.Segment> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(this);
  }

  @Override
  public void request(long n) {
    if (n <= 0) {
      if (subscriber != null) subscriber.onError(new IllegalArgumentException("Request must be > 0"));
      return;
    }
    addDemand(n);
    if (isQuerySent.compareAndSet(false, true)) {
      try {
        transport.setClientHandlers(this::messageHandler, this::errorHandler);
        transport.sendQueryMessageAsync(queryTdsMessage);
      } catch (Exception e) {
        if (subscriber != null) subscriber.onError(e);
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
    tokenDispatcher.processMessage(tdsMessage, context, queryContext, this);
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
          R2dbcException exception = createException(error);
          if (subscriber != null) subscriber.onError(exception);
          queryContext.setHasError(true);
        }
        break;
      case ENV_CHANGE:
        EnvChangeApplier.apply((EnvChangeToken) token, context);
        break;
    }

    if (segment != null) {
      buffer.offer(segment);
    }
    drain();
  }

  private void drain() {
    if (subscriber == null) return;
    if (wip.getAndIncrement() != 0) return;

    int missed = 1;
    do {
      long requested = demand.get();
      long emitted = 0;

      while (emitted != requested) {
        if (isCancelled.get()) {
          buffer.clear();
          return;
        }

        boolean done = upstreamDone.get();
        Result.Segment s = buffer.poll();
        boolean empty = (s == null);

        if (done && empty) {
          if (isCancelled.compareAndSet(false, true)) {
            subscriber.onComplete();
          }
          return;
        }

        if (empty) break;

        subscriber.onNext(s);
        emitted++;
      }

      if (emitted == requested) {
        if (isCancelled.get()) {
          buffer.clear();
          return;
        }
        boolean done = upstreamDone.get();
        if (done && buffer.isEmpty()) {
          if (isCancelled.compareAndSet(false, true)) {
            subscriber.onComplete();
          }
          return;
        }
      }

      if (emitted != 0 && requested != Long.MAX_VALUE) {
        demand.addAndGet(-emitted);
      }

      missed = wip.addAndGet(-missed);
    } while (missed != 0);
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
    return new TdsRowSegment(new TdsRow(token.getColumnData(), metaList, context.getVarcharCharset()));
  }

  private Result.OutSegment createOutSegment(QueryContext ctx) {
    List<ReturnValueToken> tokens = ctx.getReturnValues();
    List<byte[]> values = new ArrayList<>(tokens.size());
    List<TdsOutParameterMetadata> metaList = new ArrayList<>(tokens.size());

    for (ReturnValueToken token : tokens) {
      values.add((byte[]) token.getValue());
      metaList.add(new TdsOutParameterMetadata(token));
    }

    return new TdsOutSegment(new TdsOutParameters(values, metaList, context.getVarcharCharset()));
  }

  private void errorHandler(Throwable t) {
    if (subscriber != null && !isCancelled.get()) subscriber.onError(t);
  }

  public static R2dbcException createException(ErrorToken error) {
    String message = error.getMessage();
    String sqlState = String.valueOf(error.getState());
    int errorCode = (int) error.getNumber();

    return switch (errorCode) {
      case 102, 156, 170, 208 -> new R2dbcBadGrammarException(message, sqlState, errorCode);
      case 229, 262 -> new R2dbcPermissionDeniedException(message, sqlState, errorCode);
      case 547, 2601, 2627 -> new R2dbcDataIntegrityViolationException(message, sqlState, errorCode);
      case 1205 -> new R2dbcTransientResourceException(message, sqlState, errorCode);
      default -> {
        if (error.getSeverity() >= 19) {
          yield new R2dbcNonTransientResourceException(message, sqlState, errorCode);
        }
        yield new R2dbcNonTransientExceptionSubclass(message, sqlState, errorCode);
      }
    };
  }

  private static class R2dbcNonTransientExceptionSubclass extends io.r2dbc.spi.R2dbcNonTransientException {
    public R2dbcNonTransientExceptionSubclass(String reason, String sqlState, int errorCode) {
      super(reason, sqlState, errorCode);
    }
  }
}