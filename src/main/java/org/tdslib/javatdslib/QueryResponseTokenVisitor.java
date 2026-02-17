package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcTransientResourceException;
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
import org.tdslib.javatdslib.tokens.returnstatus.ReturnStatusToken;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueToken;
import org.tdslib.javatdslib.tokens.row.RowToken;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stateful visitor that collects the results as Segments (Rows, Counts, OutParams).
 * Implements Publisher<Result.Segment> for R2DBC 0.9+.
 */
public class QueryResponseTokenVisitor implements Publisher<Result.Segment>, TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(QueryResponseTokenVisitor.class);
  private static final String SERVER_MESSAGE = "Server message [{}] (state {}): {}";

  private final TdsTransport transport;
  private final TdsMessage queryTdsMessage;
  private final TokenDispatcher tokenDispatcher;

  private Subscriber<? super Result.Segment> subscriber;
  private final AtomicBoolean messageSent = new AtomicBoolean(false);

  public QueryResponseTokenVisitor(TdsTransport transport, TdsMessage queryTdsMessage) {
    this.transport = transport;
    this.queryTdsMessage = queryTdsMessage;
    this.transport.setClientHandlers(this::messageHandler, this::errorHandler);
    this.tokenDispatcher = new TokenDispatcher();
  }

  private void messageHandler(TdsMessage tdsMessage) {
    tokenDispatcher.processMessage(tdsMessage, transport, new QueryContext(), this);
    if (tdsMessage.isResetConnection()) {
      // Handle reset logic if needed
    }
  }

  private void errorHandler(Throwable throwable) {
    if (subscriber != null) {
      subscriber.onError(throwable);
    }
  }

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    switch (token.getType()) {
      case COL_METADATA:
        queryContext.setColMetaDataToken((ColMetaDataToken) token);
        break;

      case ROW:
        // 1. Build the Row
        RowToken rowToken = (RowToken) token;
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        // Guard against missing metadata (e.g., triggers with no SELECT)
        if (queryContext.getColMetaDataToken() != null) {
          for (ColumnMeta columnMeta : queryContext.getColMetaDataToken().getColumns()) {
            columnMetadataList.add(new TdsColumnMetadata(columnMeta));
          }
        }
        TdsRow row = new TdsRow(rowToken.getColumnData(), columnMetadataList);

        // 2. Emit as RowSegment
        // Assuming you have created TdsRowSegment as discussed
        subscriber.onNext(new TdsRowSegment(row));
        break;

      case DONE:
      case DONE_IN_PROC:
      case DONE_PROC:
        DoneToken done = (DoneToken) token;

        // 1. Emit Update Count if available
        if (done.getStatus().hasCount()) {
          // Assuming you have created TdsUpdateCount as discussed
          subscriber.onNext(new TdsUpdateCount(done.getCount()));
        }

        // 2. Check for End of Response
        if (!done.getStatus().hasMoreResults()) {
          if (!queryContext.isHasError()) {

            // 3. Handle OUT Parameters (Return Values)
            if (!queryContext.getReturnValues().isEmpty()) {
              int size = queryContext.getReturnValues().size();
              List<byte[]> data = new ArrayList<>(size);
              List<ColumnMetadata> columns = new ArrayList<>(size);

              for (int i = 0; i < size; i++) {
                ReturnValueToken rv = queryContext.getReturnValues().get(i);
                data.add((byte[]) rv.getValue());
                columns.add(new TdsColumnMetadata(new ColumnMeta(
                    i + 1,
                    rv.getParamName(),
                    rv.getTypeInfo().getTdsType().byteVal,
                    rv.getStatusFlags(),
                    rv.getTypeInfo()
                )));
              }

              // Wrap the data in a RowImpl, then delegate via TdsOutParameters
              TdsRow outDataRow = new TdsRow(data, columns);
              // Assuming TdsOutSegment and TdsOutParameters exist
              subscriber.onNext(new TdsOutSegment(new TdsOutParameters(outDataRow)));
            }

            subscriber.onComplete();
          }
        }
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
          R2dbcException exception = mapErrorToException(error);
          subscriber.onError(exception);
          queryContext.setHasError(true);
        }
        break;

      case ENV_CHANGE:
        transport.applyEnvChange((EnvChangeToken) token);
        break;

      case RETURN_VALUE:
        queryContext.addReturnValue((ReturnValueToken) token);
        break;

      case RETURN_STATUS:
        logger.debug("Return Status: {}", ((ReturnStatusToken) token).getValue());
        break;

      default:
        logger.debug("Ignored token: {}", token.getType());
    }
  }

  @Override
  public void subscribe(Subscriber<? super Result.Segment> subscriber) {
    if (subscriber == null) throw new NullPointerException("Subscriber cannot be null");
    this.subscriber = subscriber;

    subscriber.onSubscribe(new Subscription() {
      @Override
      public void request(long n) {
        if (!messageSent.compareAndSet(true, true)) {
          try {
            transport.sendQueryMessageAsync(queryTdsMessage);
          } catch (IOException e) {
            subscriber.onError(new RuntimeException(e));
          }
        }
      }

      @Override
      public void cancel() {
        transport.cancelCurrent();
      }
    });
  }

  private R2dbcException mapErrorToException(ErrorToken error) {
    String message = error.getMessage();
    String sqlState = String.valueOf(error.getState() & 0xFF);
    int errorCode = (int) error.getNumber();

    switch (errorCode) {
      case 1205: case -2: case 11: case 10054: case 10060:
        return new R2dbcTransientResourceException(message, sqlState, errorCode);
      case 2627: case 2601: case 547: case 515:
        return new R2dbcDataIntegrityViolationException(message, sqlState, errorCode);
      case 208: case 207: case 102: case 156:
        return new R2dbcBadGrammarException(message, sqlState, errorCode);
      case 229: case 230: case 18456:
        return new R2dbcPermissionDeniedException(message, sqlState, errorCode);
      default:
        return new R2dbcNonTransientResourceException(message, sqlState, errorCode);
    }
  }
}