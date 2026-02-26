package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.info.InfoToken;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueToken;
import org.tdslib.javatdslib.tokens.row.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.EnvChangeApplier;
import org.tdslib.javatdslib.transport.TdsTransport;
import org.tdslib.javatdslib.transport.AbstractQueueDrainPublisher;

import java.util.concurrent.atomic.AtomicBoolean;

public class QueryResponseTokenVisitor extends AbstractQueueDrainPublisher<Result.Segment> implements TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(QueryResponseTokenVisitor.class);
  private static final String SERVER_MESSAGE = "Server message [{}] (state {}): {}";

  private final TdsTransport transport;
  private final ConnectionContext context;
  private final TdsMessage queryTdsMessage;
  private final TokenDispatcher tokenDispatcher;
  private final QueryContext queryContext = new QueryContext();

  private final AtomicBoolean isQuerySent = new AtomicBoolean(false);

  public QueryResponseTokenVisitor(TdsTransport transport, ConnectionContext context, TdsMessage queryTdsMessage, TokenDispatcher tokenDispatcher) {
    this.transport = transport;
    this.context = context;
    this.queryTdsMessage = queryTdsMessage;
    this.tokenDispatcher = tokenDispatcher;
  }

  @Override
  protected void onRequest(long n) {
    // Only send the query on the very first downstream request
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
    tokenDispatcher.processMessage(tdsMessage, context, queryContext, this);
  }

  private void errorHandler(Throwable t) {
    if (!isCancelled.get()) {
      error(t);
    }
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

      case INFO:
        InfoToken info = (InfoToken) token;
        logger.info(SERVER_MESSAGE, info.getNumber(), info.getState(), info.getMessage());
        if (info.isError()) queryContext.setHasError(true);
        break;

      case ERROR:
        ErrorToken err = (ErrorToken) token;
        logger.error(SERVER_MESSAGE, err.getNumber(), err.getState(), err.getMessage());

        if (err.isError()) {
          queryContext.setHasError(true);
          error(SqlErrorTranslator.createException(err));
        }
        break;

      case ENV_CHANGE:
        EnvChangeApplier.apply((EnvChangeToken) token, context);
        break;
    }
  }
}