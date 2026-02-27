package org.tdslib.javatdslib.tokens.visitors;

import io.r2dbc.spi.Result;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.SegmentTranslator;
import org.tdslib.javatdslib.TdsUpdateCount;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueToken;
import org.tdslib.javatdslib.tokens.row.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import org.tdslib.javatdslib.transport.AbstractQueueDrainPublisher;

import java.util.concurrent.atomic.AtomicBoolean;

public class ResultSegmentVisitor extends AbstractQueueDrainPublisher<Result.Segment> implements TokenVisitor {

  private final TdsTransport transport;
  private final ConnectionContext context;
  private final TdsMessage queryTdsMessage;
  private final TokenDispatcher tokenDispatcher;
  private final QueryContext queryContext = new QueryContext();

  private TokenVisitor visitorChain; // The dynamic pipeline
  private final AtomicBoolean isQuerySent = new AtomicBoolean(false);

  public ResultSegmentVisitor(TdsTransport transport, ConnectionContext context, TdsMessage queryTdsMessage, TokenDispatcher tokenDispatcher) {
    this.transport = transport;
    this.context = context;
    this.queryTdsMessage = queryTdsMessage;
    this.tokenDispatcher = tokenDispatcher;
  }

  public void setVisitorChain(TokenVisitor visitorChain) {
    this.visitorChain = visitorChain;
  }

  public void emitStreamError(Throwable t) {
    super.error(t);
  }

  @Override
  protected void onRequest(long n) {
    if (isQuerySent.compareAndSet(false, true)) {
      try {
        transport.setClientHandlers(this::messageHandler, this::errorHandler);
        transport.sendQueryMessageAsync(queryTdsMessage);
      } catch (Exception e) {
        error(e);
      }
    }
  }

  @Override
  protected void onCancel() {
    transport.cancelCurrent();
  }

  private void messageHandler(TdsMessage tdsMessage) {
    if (isCancelled.get()) return;
    // Route tokens through the entire assembled chain
    tokenDispatcher.processMessage(tdsMessage, context, queryContext, visitorChain != null ? visitorChain : this);
  }

  private void errorHandler(Throwable t) {
    if (!isCancelled.get()) error(t);
  }

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    if (isCancelled.get()) return;

    switch (token.getType()) {
      case ROW:
        emit(SegmentTranslator.createRowSegment((RowToken) token, queryContext, context));
        break;

      case DONE:
      case DONE_IN_PROC:
      case DONE_PROC:
        DoneToken done = (DoneToken) token;
        if (!queryContext.getReturnValues().isEmpty()) {
          emit(SegmentTranslator.createOutSegment(queryContext, context));
          queryContext.getReturnValues().clear();
        } else if (done.getStatus().hasCount()) {
          emit(new TdsUpdateCount(done.getCount()));
        }

        if (!done.getStatus().hasMoreResults() && !queryContext.isHasError()) {
          complete();
        }
        break;

      case RETURN_VALUE:
        queryContext.addReturnValue((ReturnValueToken) token);
        break;
    }
  }
}