package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.tokens.StatefulTokenDecoder;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParserRegistry;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.ErrorToken;
import org.tdslib.javatdslib.tokens.models.InfoToken;
import org.tdslib.javatdslib.tokens.models.RawRowToken;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TdsResultStreamHandler extends SerializedQueueDrainer<Result.Segment> implements TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(TdsResultStreamHandler.class);
  private final TdsTransport transport;
  private final ConnectionContext context;
  private final TdsMessage queryTdsMessage;

  private TokenVisitor visitorChain;
  private final AtomicBoolean isQuerySent = new AtomicBoolean(false);

  private ColMetaDataToken currentMetaData;
  private final List<ReturnValueToken> returnValues = new ArrayList<>();
  private boolean hasError = false;

  private StatefulTokenDecoder activeDecoder;

  public TdsResultStreamHandler(
      TdsTransport transport, ConnectionContext context, TdsMessage queryTdsMessage) {
    this.transport = transport;
    this.context = context;
    this.queryTdsMessage = queryTdsMessage;
  }

  public void setVisitorChain(TokenVisitor visitorChain) {
    this.visitorChain = visitorChain;
  }

  @Override
  protected void onSourceOverrun() {
    logger.trace("[TdsResultStreamHandler] onSourceOverrun triggered by backpressure. Telling Transport to drop OP_READ.");
    if (transport != null) {
      transport.suspendNetworkRead();
    }
  }

  @Override
  protected void onSourceStarved() {
    logger.trace("[TdsResultStreamHandler] onSourceStarved triggered by demand. Telling Transport to restore OP_READ.");
    if (transport != null) {
      transport.resumeNetworkRead();
    }
  }

  @Override
  protected void onRequest(long n) {
    if (isQuerySent.compareAndSet(false, true)) {
      try {
        logger.trace("[TdsResultStreamHandler] Sending initial query to server.");
        TokenVisitor pipeline = visitorChain != null ? visitorChain : this;

        activeDecoder = new StatefulTokenDecoder(
            TokenParserRegistry.DEFAULT,
            context,
            transport,
            null,
            null
        );

        transport.setStreamHandlers(activeDecoder, this::errorHandler);
        transport.sendQueryMessageAsync(queryTdsMessage);
      } catch (Exception e) {
        error(e);
      }
    }
  }

  @Override
  protected void onCancel() {
    if (transport != null) {
      transport.cancelCurrent();
    }
  }

  private void errorHandler(Throwable t) {
    if (!isCancelled()) {
      error(t);
    }
  }

  @Override
  public void onToken(Token token) {
    if (isCancelled()) return;

    if (token instanceof ColMetaDataToken) {
      logger.trace("[TdsResultStreamHandler] Received ColMetaDataToken.");
      this.currentMetaData = (ColMetaDataToken) token;

    } else if (token instanceof RawRowToken) {
      logger.trace("[TdsResultStreamHandler] Received RawRowToken. Creating StatefulRow and Emitting.");
      RawRowToken rawRow = (RawRowToken) token;

      StatefulRow row = new StatefulRow(rawRow.getPayload(), currentMetaData, transport, activeDecoder, context);

      offer(new Result.RowSegment() {
        @Override public Row row() { return row; }
      });

    } else if (token instanceof DoneToken) {
      DoneToken done = (DoneToken) token;
      if (!this.returnValues.isEmpty()) {
        offer(SegmentTranslator.createOutSegment(this.returnValues, context));
        this.returnValues.clear();
      } else if (done.getStatus().hasCount()) {
        offer(new TdsUpdateCount(done.getCount()));
      }

      if (!done.getStatus().hasMoreResults() && !this.hasError) {
        logger.trace(">>> VISITOR: DONE Token parsed successfully! Triggering complete()!");
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

  @Override
  public void onError(Throwable t) {
    this.hasError = true;
    error(t);
  }
}