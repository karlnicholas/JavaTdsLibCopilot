package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.protocol.CollationUtils;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.StatefulTokenDecoder;
import org.tdslib.javatdslib.tokens.Token;
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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

  // NEW: Keep a reference to the active decoder
  private StatefulTokenDecoder activeDecoder;

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
        TokenVisitor pipeline = visitorChain != null ? visitorChain : this;

        // NEW: Save the instance to the class field
        this.activeDecoder = new StatefulTokenDecoder(
            TokenParserRegistry.DEFAULT,
            context,
            pipeline,
            transport // (Assumes StatefulTokenDecoder was updated to accept transport)
        );

        transport.setStreamHandlers(this.activeDecoder, this::errorHandler);
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

    if (token instanceof ColMetaDataToken) {
      this.currentMetaData = (ColMetaDataToken) token;

    } else if (token instanceof RawRowToken) {
      RawRowToken rawRow = (RawRowToken) token;

      // FIX: Expect a List of Objects, not just byte arrays
      List<Object> columnData = parseRowBytes(rawRow.getPayload(), currentMetaData);

      // Note: You will need to ensure SegmentTranslator.createRowSegment
      // is updated to accept List<Object> instead of List<byte[]>
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

  // Update the parseRowBytes method:
  private List<Object> parseRowBytes(ByteBuffer payload, ColMetaDataToken metaData) {
    List<Object> rowData = new ArrayList<>();
    if (metaData != null) {
      for (ColumnMeta col : metaData.getColumns()) {
        TdsType type = TdsType.valueOf(col.getDataType());

        // --- Dynamically resolve the Charset ---
        Charset charset = StandardCharsets.UTF_16LE; // Default for NVARCHAR and XML
        if (type == TdsType.BIGVARCHR || type == TdsType.VARCHAR || type == TdsType.TEXT) {
          byte[] collation = col.getTypeInfo() != null ? col.getTypeInfo().getCollation() : null;
          charset = collation != null
              ? CollationUtils.getCharsetFromCollation(collation).orElse(context.getVarcharCharset())
              : context.getVarcharCharset();
        }

        // Pass the charset down to DataParser
        Object data = DataParser.getDataBytes(payload, type, col.getMaxLength(), transport, activeDecoder, charset);
        rowData.add(data);
      }
    }
    return rowData;
  }
}
