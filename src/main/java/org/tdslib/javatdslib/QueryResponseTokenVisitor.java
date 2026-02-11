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
import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.info.InfoToken;
import org.tdslib.javatdslib.tokens.returnstatus.ReturnStatusToken;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueToken;
import org.tdslib.javatdslib.tokens.row.RowToken;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stateful visitor that collects the results of one or more result-sets
 * from a SQL batch execution.
 */
public class QueryResponseTokenVisitor implements Publisher<Row>, TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(QueryResponseTokenVisitor.class);
  private static final String SERVER_MESSAGE = "Server message [{}] (state {}): {}";

  private final TdsTransport transport;
  private final TdsMessage queryTdsMessage;
  private final TokenDispatcher tokenDispatcher;

  // ------------------------------------------------

  private Subscriber<? super Row> subscriber;
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
      // resetToDefaults();
    }

  }

  private void errorHandler(Throwable throwable) {
    subscriber.onError(throwable);
  }

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    switch (token.getType()) {
      case COL_METADATA:
        queryContext.setColMetaDataToken((ColMetaDataToken) token);
        break;

      case ROW:
        // Push the row data into the result's internal stream
        RowToken rowToken = (RowToken) token;
        TdsRowImpl rowRow = new TdsRowImpl(rowToken.getColumnData(), queryContext.getColMetaDataToken().getColumns());
        subscriber.onNext(rowRow);
        break;
      case DONE:
      case DONE_IN_PROC:
      case DONE_PROC:
        DoneToken done = (DoneToken) token;
        // 2. Check for More Result Sets (Mask 0x01)
        if (!done.getStatus().hasMoreResults()) {
          // No more results = truly done
          if (!queryContext.isHasError()) {
            if (!queryContext.getReturnValues().isEmpty()) {
              // 1. Optimization: Pre-size the lists to avoid resizing overhead
              int size = queryContext.getReturnValues().size();
              List<byte[]> data = new ArrayList<>(size);
              List<ColumnMeta> columns = new ArrayList<>(size);

              for (int i = 0; i < size; i++) {
                ReturnValueToken rv = queryContext.getReturnValues().get(i);

                // Cast securely if getValue() returns Object
                data.add((byte[]) rv.getValue());

                // 2. Fix: Use dynamic column index (i + 1)
                columns.add(new ColumnMeta(
                    i + 1,
                    rv.getParamName(),
                    rv.getTypeInfo().getTdsType().byteVal,
                    rv.getStatusFlags(),
                    rv.getTypeInfo()
                ));
              }

              subscriber.onNext(new TdsRowImpl(data, columns));
            }
            subscriber.onComplete();
            logger.debug("fired onComplete");
          }
        }
        break;
      case INFO:
        InfoToken info = (InfoToken) token;
        logger.info(SERVER_MESSAGE, info.getNumber(), info.getState(), info.getMessage());

        // should probably fire onError
        if (info.isError()) {
          queryContext.setHasError(true);
        }
        break;
      case ERROR:
        ErrorToken error = (ErrorToken) token;
        logger.error(SERVER_MESSAGE, error.getNumber(), error.getState(), error.getMessage());

        if (error.isError()) {
          // 1. Extract and sanitize values
          String message = error.getMessage();
          // Convert raw state byte to string (masking to unsigned for clarity)
          String sqlState = String.valueOf(error.getState() & 0xFF);
          // Safe cast: SQL Server error numbers fit in integer
          int errorCode = (int) error.getNumber();

          R2dbcException exception;

          // 2. Map SQL Server Error Numbers to R2DBC Exception Types
          switch (errorCode) {
            case 1205: // Deadlock victim
            case -2:   // Timeout (client-side driver often uses negative numbers)
            case 11:   // General network error
            case 10054:// Connection reset by peer
            case 10060:// Connection timed out
              exception = new R2dbcTransientResourceException(message, sqlState, errorCode);
              break;

            case 2627: // Violation of PRIMARY KEY constraint
            case 2601: // Cannot insert duplicate key row
            case 547:  // The INSERT statement conflicted with the FOREIGN KEY constraint
            case 515:  // Cannot insert the value NULL into column
              exception = new R2dbcDataIntegrityViolationException(message, sqlState, errorCode);
              break;

            case 208: // Invalid object name (Table not found)
            case 207: // Invalid column name
            case 102: // Incorrect syntax near ...
            case 156: // Incorrect syntax near the keyword ...
              exception = new R2dbcBadGrammarException(message, sqlState, errorCode);
              break;

            case 229: // The permission was denied on the object...
            case 230: // The permission was denied on the column...
            case 18456:// Login failed for user
              exception = new R2dbcPermissionDeniedException(message, sqlState, errorCode);
              break;

            default:
              // Fallback for all other errors (assumed permanent/fatal)
              exception = new R2dbcNonTransientResourceException(message, sqlState, errorCode);
              break;
          }

          // 3. Terminate the stream
          subscriber.onError(exception);
          logger.debug("fired onError");
          queryContext.setHasError(true);
        }
        break;

      case ENV_CHANGE:
        transport.applyEnvChange((EnvChangeToken) token);
        break;

      case RETURN_STATUS:
        ReturnStatusToken returnStatusToken = (ReturnStatusToken) token;
        logger.info("Server return status: {}", returnStatusToken.getValue());
        break;

      case RETURN_VALUE:
        queryContext.addReturnValue((ReturnValueToken) token);
        break;

      default:
        logger.warn("Unhandled token type: {}", token.getType());
    }
  }

  @Override
  public void subscribe(Subscriber<? super Row> subscriber) {
    // Validate subscriber
    if (subscriber == null) {
      throw new NullPointerException("Subscriber cannot be null");
    }

    this.subscriber = subscriber;
    subscriber.onSubscribe(new Subscription() {

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

  public Subscriber<? super Row> getSubscriber() {
    return subscriber;
  }
}
