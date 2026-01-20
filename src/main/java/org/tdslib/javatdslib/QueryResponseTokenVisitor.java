package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.done.DoneStatus;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.info.InfoToken;
import org.tdslib.javatdslib.tokens.row.RowToken;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.io.IOException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stateful visitor that collects the results of one or more result-sets
 * from a SQL batch execution.
 */
public class QueryResponseTokenVisitor implements Flow.Publisher<RowWithMetadata>, TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(QueryResponseTokenVisitor.class);
  private static final String SERVER_MESSAGE = "Server message [{}] (state {}): {}";

  private final TdsTransport transport;
  private final TdsMessage queryTdsMessage;
  private final TokenDispatcher tokenDispatcher;

  // ------------------- State -------------------
  private ColMetaDataToken currentMetadata;          // last seen COL_METADATA
  private boolean hasError = false;
  // ------------------------------------------------

  private Flow.Subscriber<? super RowWithMetadata> subscriber;
  private final AtomicBoolean messageSent = new AtomicBoolean(false);

  /**
   * Create a new QueryResponseTokenVisitor that will apply ENVCHANGE tokens to
   * the provided ConnectionContext and collect result sets produced by tokens.
   *
   */
  public QueryResponseTokenVisitor(
          TdsTransport transport,
          TdsMessage queryTdsMessage) {
    this.transport = transport;
    this.queryTdsMessage = queryTdsMessage;
    this.transport.setClientHandlers(this::messageHander, this::errorHandler);
    this.tokenDispatcher = new TokenDispatcher();
  }

  /**
   * Invoked by the transport when a TDS tdsMessage arrives.
   *
   * <p>This method dispatches tokens contained in the provided tdsMessage to the
   * currently active token visitor (via {@link TokenDispatcher}). The visitor
   * is responsible for handling token-level semantics (ENVCHANGE, LOGINACK,
   * row publishing, errors, etc.). After dispatching, if the tdsMessage signals
   * a connection-level reset (resetConnection flag) the client's session state
   * is reset to library defaults.
   *
   * @param tdsMessage the received {@link TdsMessage}; expected to be non-null and
   *                containing tokens to process
   */
  private void messageHander(TdsMessage tdsMessage) {

    // Dispatch tokens to the visitor (which handles ENVCHANGE, errors, etc.)
    tokenDispatcher.processMessage(tdsMessage, transport, new QueryContext(), this);

    // Still handle reset flag separately (visitor doesn't cover tdsMessage-level flags)
    if (tdsMessage.isResetConnection()) {
//      resetToDefaults();
    }

  }

  private void errorHandler(Throwable throwable) {
    getSubscriber().onError(throwable);
  }

  @Override
  public void onToken(Token token) {
    switch (token.getType()) {
      case COL_METADATA:
        currentMetadata = (ColMetaDataToken) token;
        break;

      case ROW:
        if (currentMetadata == null) {
          logger.warn("ROW token without preceding COL_METADATA - ignoring");
          return;
        }
        RowToken row = (RowToken) token;
        subscriber.onNext(new RowWithMetadata(row.getColumnData(), currentMetadata.getColumns()));
        logger.trace("Row added ({} columns)", row.getColumnData().size());
        break;

      case DONE:
      case DONE_IN_PROC:
      case DONE_PROC:
        DoneToken done = (DoneToken) token;
        DoneStatus status = done.getStatus(); // Assuming DoneToken wraps the short in DoneStatus
        // 2. Check for More Result Sets (Mask 0x01)
        if (!status.hasMoreResults()) {
          // No more results = truly done
          subscriber.onComplete();
        }
        break;

      case INFO:
        InfoToken info = (InfoToken) token;
        logger.info(SERVER_MESSAGE, info.getNumber(), info.getState(), info.getMessage());

        if (info.isError()) {
          hasError = true;
        }
        break;
      case ERROR:
        ErrorToken error = (ErrorToken) token;
        logger.info(SERVER_MESSAGE, error.getNumber(), error.getState(), error.getMessage());

        if (error.isError()) {
          hasError = true;
        }
        break;

      case ENV_CHANGE:
        transport.applyEnvChange((EnvChangeToken) token);
        break;

      default:
        logger.warn("Unhandled token type: {}", token.getType());
    }
  }

  /**
   * Indicates whether any server error/info tokens flagged an error during processing.
   *
   * @return {@code true} if an error was observed
   */
  public boolean hasError() {
    return hasError;
  }

  @Override
  public void subscribe(Flow.Subscriber<? super RowWithMetadata> subscriber) {
    // Validate subscriber
    if (subscriber == null) {
      throw new NullPointerException("Subscriber cannot be null");
    }

    this.subscriber = subscriber;
    subscriber.onSubscribe(new Flow.Subscription() {

      @Override
      public void request(long n) {
        if (!messageSent.compareAndSet(true, true)) {
          try {
            transport.sendQueryMessageAsync(queryTdsMessage);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public void cancel() {
        transport.cancelCurrent();
      }
    });
    // Store the current subscriber for use in the response handling

  }

  public Flow.Subscriber<? super RowWithMetadata> getSubscriber() {
    return subscriber;
  }
}
