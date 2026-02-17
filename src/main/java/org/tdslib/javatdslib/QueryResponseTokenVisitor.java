package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.ColumnMetadata;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataToken;
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


  private final TdsTransport transport;
  private final TdsMessage queryTdsMessage;
  private final TokenDispatcher tokenDispatcher;

  // Subscription State
  private Subscriber<? super Result.Segment> subscriber;
  private final AtomicBoolean isQuerySent = new AtomicBoolean(false);
  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicBoolean upstreamDone = new AtomicBoolean(false);

  // Buffer for when network pushes data faster than downstream requests it
  private final Queue<Result.Segment> buffer = new ConcurrentLinkedQueue<>();

  public QueryResponseTokenVisitor(TdsTransport transport, TdsMessage queryTdsMessage) {
    this.transport = transport;
    this.queryTdsMessage = queryTdsMessage;
    this.tokenDispatcher = new TokenDispatcher();
    // Register this visitor to handle the callbacks
    this.transport.setClientHandlers(this::messageHandler, this::errorHandler);
  }

  // --- Publisher Implementation ---
  @Override
  public void subscribe(Subscriber<? super Result.Segment> subscriber) {
    this.subscriber = subscriber;
    // Pass 'this' as the Subscription, giving us control over request() logic
    subscriber.onSubscribe(this);
  }

  // --- Subscription Implementation ---
  @Override
  public void request(long n) {
    if (n <= 0) {
      subscriber.onError(new IllegalArgumentException("Request must be > 0"));
      return;
    }

    // 1. Update Demand (safely add to AtomicLong)
    addDemand(n);

    // 2. Trigger Execution (ONLY ONCE)
    if (isQuerySent.compareAndSet(false, true)) {
      try {
        transport.sendQueryMessageAsync(queryTdsMessage);
      } catch (IOException e) {
        subscriber.onError(e);
      }
    } else {
      // 3. If query already sent, drain any buffered items meant for this new demand
      drain();
    }
  }

  @Override
  public void cancel() {
    isCancelled.set(true);
    transport.cancelCurrent();
  }

  // --- Token Processing (The Network Push) ---
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
        // 1. Capture the Count
        if (done.getStatus().hasCount()) {
          segment = new TdsUpdateCount(done.getCount());
        }
        // 2. Check for Completion (Defer it!)
        if (!done.getStatus().hasMoreResults() && !queryContext.isHasError()) {
          upstreamDone.set(true);
        }
        break;
      case ENV_CHANGE:
        transport.applyEnvChange((EnvChangeToken) token);
        break;
      case RETURN_VALUE:
        queryContext.addReturnValue((ReturnValueToken) token);
        break;
    }
    // Emit or Buffer
    logger.debug("Emitting segment: {}, {}", Thread.currentThread().getName(), segment);
    if (segment != null) {
      buffer.offer(segment);
    }

    // Always attempt to drain after processing a token
    drain();
  }

  // REMOVE/UPDATE createDoneSegment to stop calling onComplete directly
  private Result.Segment createDoneSegment(DoneToken token, QueryContext ctx) {
    // This method is now effectively inlined in onToken for clarity,
    // or you can keep it strictly for returning the segment,
    // BUT REMOVE THE subscriber.onComplete() CALL FROM IT.
    throw new UnsupportedOperationException("Logic moved to onToken to fix ordering");
  }

  // REPLACE existing drain() with this Spec-Compliant version
  private void drain() {
    if (subscriber == null) return;

    // A simple lock-free loop is risky for recursion depth.
    // Ideally use a WIP integer, but for now, ensure we check order.

    // 1. Emit buffered items if requested
    while (!buffer.isEmpty() && demand.get() > 0) {
      Result.Segment s = buffer.poll();
      if (s != null) {
        subscriber.onNext(s);
        demand.decrementAndGet();
      }
    }

    // 2. ONLY complete if buffer is empty AND upstream is done
    if (upstreamDone.get() && buffer.isEmpty()) {
      // Ensure we only call onComplete once
      if (isCancelled.compareAndSet(false, true)) {
        subscriber.onComplete();
      }
    }
  }

  private void addDemand(long n) {
    // Safe atomic addition (cap at Long.MAX_VALUE)
    long current, next;
    do {
      current = demand.get();
      next = current + n;
      if (next < 0) next = Long.MAX_VALUE; // Overflow check
    } while (!demand.compareAndSet(current, next));
  }

  // --- Factory Methods for Segments (Clean up the switch statement) ---
  private Result.Segment createRowSegment(RowToken token, QueryContext ctx) {
    List<ColumnMetadata> metaList = new ArrayList<>();
    if (ctx.getColMetaDataToken() != null) {
      for (ColumnMeta cm : ctx.getColMetaDataToken().getColumns()) {
        metaList.add(new TdsColumnMetadata(cm));
      }
    }
    return new TdsRowSegment(new TdsRow(token.getColumnData(), metaList));
  }

  private void errorHandler(Throwable t) {
    if (subscriber != null && !isCancelled.get()) subscriber.onError(t);
  }
}