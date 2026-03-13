package org.tdslib.javatdslib.tokens;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

public class StatefulTokenDecoder implements TdsStreamHandler {
  private static final Logger logger = LoggerFactory.getLogger(StatefulTokenDecoder.class);

  private final TokenParserRegistry registry;
  private final ConnectionContext context;
  private final TdsTransport transport;
  private final TdsDecoderSink sink;
  private final List<List<TdsParameter>> executions;

  // Inline Accumulator
  private ByteBuffer accumulator;

  // Parsing State
  private ColMetaDataToken currentMetaData;
  private boolean expectingNewToken = true;
  private byte currentTokenType = 0;

  // Row Parsing State
  private int currentRowColIndex = -1;
  private long pendingPlpChunkBytes = 0;
  private boolean expectingPlpChunkLength = false;

  public StatefulTokenDecoder(
      TokenParserRegistry registry,
      ConnectionContext context,
      TdsTransport transport,
      TdsDecoderSink sink,
      List<List<TdsParameter>> executions) {
    this.registry = registry;
    this.context = context;
    this.transport = transport;
    this.sink = sink;
    this.executions = executions;
    accumulator = ByteBuffer.allocate(context.getCurrentPacketSize() * 2).order(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public synchronized void onPayloadAvailable(ByteBuffer chunk, boolean isEom) {
    try {
      // 1. Concatenate new data
      ensureCapacity(chunk.remaining());
      accumulator.put(chunk);
      accumulator.flip();

      // 2. Parse as much as possible
      while (accumulator.hasRemaining()) {
        accumulator.mark(); // Mark start of current token/column attempt

        if (currentRowColIndex >= 0) {
          // We are in the middle of parsing a ROW
          if (!parseRowColumns()) {
            accumulator.reset(); // Not enough bytes, wait for next payload
            break;
          }
        } else {
          // We are expecting a new TDS Token
          if (expectingNewToken) {
            currentTokenType = accumulator.get();
            expectingNewToken = false;
          }

          if (currentTokenType == TokenType.ROW.getValue()) {
            // Enter Row Parsing Mode
            if (currentMetaData == null) {
              throw new IllegalStateException("Received ROW token before ColMetaData.");
            }
            currentRowColIndex = 0; // Start at first column
            if (!parseRowColumns()) {
              accumulator.reset();
              break;
            }
          } else {
            // Standard Token Parsing
            TokenParser parser = registry.getParser(currentTokenType);
            if (parser == null) {
              throw new IllegalStateException(String.format("Unknown token type: 0x%02X", currentTokenType));
            }

            Token token = parser.parse(accumulator, currentTokenType, context);

            if (token instanceof ColMetaDataToken meta) {
              this.currentMetaData = meta; // Capture dynamically
            }

            sink.onToken(token);
            expectingNewToken = true;
          }
        }
      }
      accumulator.compact(); // Preserve unread bytes for next call
    } catch (BufferUnderflowException e) {
      // Safe fallback if a standard token parser underflows
      accumulator.reset();
      accumulator.compact();
    } catch (Exception e) {
      logger.error("Fatal error during token decoding", e);
      sink.onError(e);
    }
  }

  /**
   * Attempts to parse columns for the current row.
   * Returns true if finished with the row or column, false if waiting for more bytes.
   */
  private boolean parseRowColumns() {
    int columnCount = currentMetaData.getColumns().size();

    while (currentRowColIndex < columnCount) {
      accumulator.mark(); // Mark per column to avoid re-reading massive PLP data

      try {
        // TODO: Determine column type from currentMetaData.getColumns().get(currentRowColIndex)
        int dataTypeCategory = 1; // 1=Fixed, 2=VarLen, 3=PLP (Mocked for structure)

        if (dataTypeCategory == 1 || dataTypeCategory == 2) {
          if (!parseStandardColumn(dataTypeCategory)) return false;
        } else if (dataTypeCategory == 3) {
          if (!parsePlpColumn()) return false;
        }

        currentRowColIndex++; // Move to next column
      } catch (BufferUnderflowException e) {
        accumulator.reset();
        return false; // Wait for more network data
      }
    }

    // Row complete
    currentRowColIndex = -1;
    expectingNewToken = true;
    return true;
  }

  private boolean parseStandardColumn(int category) {
    // Mock logic: Read length, then read bytes
    int length = category == 1 ? 4 : accumulator.getShort(); // Fixed vs VarLen
    if (accumulator.remaining() < length) {
      throw new BufferUnderflowException();
    }

    byte[] data = new byte[length];
    accumulator.get(data);

    sink.onColumnData(new CompleteDataColumn(currentRowColIndex, data));
    return true;
  }

  private boolean parsePlpColumn() {
    // PLP Streams are broken into chunks
    if (expectingPlpChunkLength || pendingPlpChunkBytes == 0) {
      if (accumulator.remaining() < 4) throw new BufferUnderflowException();
      pendingPlpChunkBytes = accumulator.getInt(); // 4-byte chunk length
      expectingPlpChunkLength = false;

      if (pendingPlpChunkBytes == 0) {
        // PLP Terminator reached
        sink.onColumnData(new PartialDataColumn(currentRowColIndex, new byte[0], true));
        expectingPlpChunkLength = true; // Reset for next time
        return true;
      }
    }

    int bytesToRead = (int) Math.min(accumulator.remaining(), pendingPlpChunkBytes);
    if (bytesToRead == 0) throw new BufferUnderflowException(); // Need at least some data

    byte[] chunkData = new byte[bytesToRead];
    accumulator.get(chunkData);
    pendingPlpChunkBytes -= bytesToRead;

    boolean isClobOrBlob = checkExecutionBindingsForStreaming(currentRowColIndex);

    if (isClobOrBlob) {
      sink.onColumnData(new PartialDataColumn(currentRowColIndex, chunkData, false));
    } else {
      // If the user bound to String/ByteBuffer, we should technically accumulate it
      // into a single CompleteDataColumn, but since PLP can be 2GB, you might
      // still want to emit partials and let the draining layer concatenate it.
      // For now, emitting as partial to prevent OOM in the network layer.
      sink.onColumnData(new PartialDataColumn(currentRowColIndex, chunkData, false));
    }

    if (pendingPlpChunkBytes == 0) {
      expectingPlpChunkLength = true; // Ready for next chunk length
    }

    // We return true here even if the PLP column isn't fully done,
    // because we successfully parsed *a chunk* and want the loop to continue.
    // Wait, if it's not done, we shouldn't increment currentRowColIndex!
    // So we return false to break the column loop but keep currentRowColIndex the same.
    return false;
  }

  private boolean checkExecutionBindingsForStreaming(int colIndex) {
    if (executions == null || executions.isEmpty()) return false;
    // In a real scenario, you'd match the colIndex to the current result set's parameter bindings
    // to see if the client requested java.sql.Clob / java.sql.Blob
    return false;
  }

  private void ensureCapacity(int neededBytes) {
    if (accumulator.remaining() < neededBytes) {
      ByteBuffer expanded = ByteBuffer.allocate(accumulator.capacity() + neededBytes + 8192).order(ByteOrder.LITTLE_ENDIAN);
      accumulator.flip();
      expanded.put(accumulator);
      accumulator = expanded;
    }
  }
}