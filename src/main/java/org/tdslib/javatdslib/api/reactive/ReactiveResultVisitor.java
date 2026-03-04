package org.tdslib.javatdslib.api.reactive;

import io.r2dbc.spi.Result;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.tdslib.javatdslib.api.SegmentTranslator;
import org.tdslib.javatdslib.api.TdsUpdateCount;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.reactive.AbstractQueueDrainPublisher;
import org.tdslib.javatdslib.tokens.StatefulTokenDecoder;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenParserRegistry;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.ErrorToken;
import org.tdslib.javatdslib.tokens.models.InfoToken;
import org.tdslib.javatdslib.tokens.models.RawRowToken;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;
import org.tdslib.javatdslib.tokens.parsers.DataParser;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * A reactive visitor that processes TDS tokens and translates them into R2DBC {@link Result.Segment}s.
 * This class acts as a bridge between the low-level TDS protocol and the reactive R2DBC API,
 * emitting row data, update counts, and other results as they are received from the database.
 */
public class ReactiveResultVisitor extends AbstractQueueDrainPublisher<Result.Segment>
    implements TokenVisitor {

  private final TdsTransport transport;
  private final ConnectionContext context;
  private final TdsMessage queryTdsMessage;

  private TokenVisitor visitorChain;
  private final AtomicBoolean isQuerySent = new AtomicBoolean(false);

  // --- Stateful Pipeline Variables ---
  private ColMetaDataToken currentMetaData;
  private final List<ReturnValueToken> returnValues = new ArrayList<>();
  private boolean hasError = false;

  /**
   * Constructs a new ReactiveResultVisitor.
   *
   * @param transport       The TDS transport layer for sending and receiving messages.
   * @param context         The connection context, containing session-specific information.
   * @param queryTdsMessage The initial TDS query message to be sent.
   */
  public ReactiveResultVisitor(
      TdsTransport transport,
      ConnectionContext context,
      TdsMessage queryTdsMessage) {
    this.transport = transport;
    this.context = context;
    this.queryTdsMessage = queryTdsMessage;
  }

  /**
   * Sets an optional next visitor in the chain to process tokens. If set, tokens will be forwarded
   * to this visitor instead of being processed by this class.
   *
   * @param visitorChain The next token visitor in the processing chain.
   */
  public void setVisitorChain(TokenVisitor visitorChain) {
    this.visitorChain = visitorChain;
  }

  /**
   * Emits an error to the downstream subscriber.
   *
   * @param t The throwable to emit.
   */
  public void emitStreamError(Throwable t) {
    super.error(t);
  }

  @Override
  protected void onRequest(long n) {
    if (isQuerySent.compareAndSet(false, true)) {
      try {
        // 1. Determine the top of the visitor chain
        TokenVisitor pipeline = visitorChain != null ? visitorChain : this;

        // 2. Create the new Stateful Decoder
        StatefulTokenDecoder decoder = new StatefulTokenDecoder(
            TokenParserRegistry.DEFAULT,
            context,
            pipeline
        );

        // 3. Wire the decoder to the Transport using the new Stream methods
        transport.setStreamHandlers(decoder, this::errorHandler);

        // 4. Send the query
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

  private void errorHandler(Throwable t) {
    if (!isCancelled.get()) {
      error(t);
    }
  }

  @Override
  public void onToken(Token token) {
    if (isCancelled.get()) {
      return;
    }

    // FIX: Swapped enum switch for instanceof checks to safely cast and avoid enum qualification
    // errors
    if (token instanceof ColMetaDataToken) {
      this.currentMetaData = (ColMetaDataToken) token;

    } else if (token instanceof RawRowToken) {
      RawRowToken rawRow = (RawRowToken) token;
      List<byte[]> columnData = parseRowBytes(rawRow.getPayload(), currentMetaData);
      emit(SegmentTranslator.createRowSegment(columnData, currentMetaData, context));

    } else if (token instanceof DoneToken) {
      DoneToken done = (DoneToken) token;
      if (!this.returnValues.isEmpty()) {
        emit(SegmentTranslator.createOutSegment(this.returnValues, context));
        this.returnValues.clear();
      } else if (done.getStatus().hasCount()) {
        emit(new TdsUpdateCount(done.getCount()));
      }

      if (!done.getStatus().hasMoreResults() && !this.hasError) {
        complete();
      }

    } else if (token instanceof ReturnValueToken) {
      this.returnValues.add((ReturnValueToken) token);

    } else if (token instanceof ErrorToken) {
      ErrorToken err = (ErrorToken) token;
      if (err.isError()) {
        this.hasError = true;
      }

    } else if (token instanceof InfoToken) {
      InfoToken info = (InfoToken) token;
      if (info.isError()) {
        this.hasError = true;
      }
    }
  }

  private List<byte[]> parseRowBytes(ByteBuffer payload, ColMetaDataToken metaData) {
    List<byte[]> rowData = new ArrayList<>();
    if (metaData != null) {
      for (ColumnMeta col : metaData.getColumns()) {
        TdsType type = TdsType.valueOf(col.getDataType());
        byte[] data = DataParser.getDataBytes(payload, type, col.getMaxLength());
        rowData.add(data);
      }
    }
    return rowData;
  }
}
