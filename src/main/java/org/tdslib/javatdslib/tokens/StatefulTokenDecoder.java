package org.tdslib.javatdslib.tokens;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.tokens.models.RowToken;
import org.tdslib.javatdslib.tokens.parsers.ColumnLengthResolver;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsStreamHandler;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class StatefulTokenDecoder implements TdsStreamHandler {
  private static final Logger logger = LoggerFactory.getLogger(StatefulTokenDecoder.class);

  private final TokenParserRegistry registry;
  private final ConnectionContext context;
  private final TdsDecoderSink sink;
  private final List<List<TdsParameter>> executions;

  private ByteBuffer accumulator;
  private ColMetaDataToken currentMetaData;
  private boolean expectingNewToken = true;
  private byte currentTokenType = 0;

  private int currentRowColIndex = -1;
  private long pendingPlpChunkBytes = 0;
  private boolean expectingPlpChunkLength = false;
  // Flag to track if we've read the 8-byte PLP total length header yet
  private boolean expectingPlpTotalLengthHeader = false;

  public StatefulTokenDecoder(
      TokenParserRegistry registry,
      ConnectionContext context,
      TdsDecoderSink sink,
      List<List<TdsParameter>> executions) {
    this.registry = registry;
    this.context = context;
    this.sink = sink;
    this.executions = executions;
    accumulator = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public synchronized void onPayloadAvailable(ByteBuffer chunk, boolean isEom) {
    try {
      ensureCapacity(chunk.remaining());
      accumulator.put(chunk);
      accumulator.flip();

      while (accumulator.hasRemaining()) {
        if (currentRowColIndex >= 0) {
          // 1. We are already inside a ROW.
          // parseRowColumns() manages its own state and standard column mark/resets.
          if (!parseRowColumns()) {
            break; // Safely yield. Do NOT reset the accumulator.
          }
        } else {
          // 2. We are expecting a new protocol Token
          accumulator.mark();

          if (expectingNewToken) {
            currentTokenType = accumulator.get();
            expectingNewToken = false;
          }

          if (currentTokenType == TokenType.ROW.getValue()) {
            if (currentMetaData == null) {
              throw new IllegalStateException("Received ROW token before ColMetaData.");
            }

            // Enter Row Parsing Mode
            currentRowColIndex = 0;
            expectingPlpTotalLengthHeader = true;

            // EMIT THE ROW START SIGNAL TO THE SINK
            sink.onToken(new RowToken(currentMetaData));

            if (!parseRowColumns()) {
              // Yield to wait for more data.
              // CRITICAL: We do NOT reset the accumulator here. The 0xD1 byte
              // and any parsed PLP chunks are permanently consumed.
              break;
            }
          } else {
            // 3. Standard Token Parsing (Hybrid Approach)
            TokenParser parser = registry.getParser(currentTokenType);
            if (parser == null) {
              logger.error(generateCrashDump(accumulator, currentTokenType));
              throw new IllegalStateException(String.format("Unknown token type: 0x%02X", currentTokenType));
            }

            // Create a read-only peek buffer to check boundaries safely
            ByteBuffer peekBuffer = accumulator.duplicate().order(ByteOrder.LITTLE_ENDIAN);

            // NEW BOOLEAN CHECK
            if (!parser.canParse(peekBuffer, context)) {
              // Not enough bytes to parse the token.
              accumulator.reset();
              expectingNewToken = true; // Reset the state flag
              break;
            }

            // We safely parse without the try-catch safety net
            Token token = parser.parse(accumulator, currentTokenType, context);

            if (token instanceof ColMetaDataToken meta) {
              this.currentMetaData = meta;
            }

            sink.onToken(token);
            expectingNewToken = true;
          }
        }
      }
      // Preserve unread bytes for next packet
      accumulator.compact();
    } catch (Exception e) {
      logger.error("Fatal error during token decoding", e);
      sink.onError(e);
    }
  }

  private boolean parseRowColumns() {
    int columnCount = currentMetaData.getColumns().size();

    while (currentRowColIndex < columnCount) {
      ColumnMeta colMeta = currentMetaData.getColumns().get(currentRowColIndex);
      TdsType tdsType = colMeta.getTypeInfo().getTdsType();

      // 1. Determine if this column requires PLP chunking
      boolean isPlp = tdsType.strategy == TdsType.LengthStrategy.PLP ||
          (tdsType.strategy == TdsType.LengthStrategy.USHORTLEN && colMeta.getMaxLength() == 65535);

      if (isPlp) {
        // PLP tracks its own byte consumption incrementally.
        // Do NOT use mark/reset here, otherwise we'd re-emit chunks on network boundaries.
        if (!parsePlpColumn(colMeta)) {
          return false; // Wait for more data. Progress is safely preserved.
        }
      } else {
        // Standard columns are all-or-nothing. Safe to mark and reset on underflow.
        accumulator.mark();

        // NO TRY-CATCH NEEDED ANYMORE!
        if (!parseStandardColumn(colMeta, tdsType)) {
          accumulator.reset(); // REWIND: Standard column was split across packets
          return false;
        }
      }

      // Reset PLP header flag for the next column if applicable
      expectingPlpTotalLengthHeader = true;
      currentRowColIndex++;
    }

    // Row complete
    currentRowColIndex = -1;
    expectingNewToken = true;
    return true;
  }

  private boolean parseStandardColumn(ColumnMeta colMeta, TdsType tdsType) {
    // 1. Resolve length using extracted utility
    int length = ColumnLengthResolver.resolveStandardLength(accumulator, tdsType, colMeta.getMaxLength());

    // NEW: Check for the incomplete header signal
    if (length == ColumnLengthResolver.INCOMPLETE_HEADER) {
      return false; // Header was sliced across packets. Yield gracefully.
    }

    if (length == -1) {
      // Null column
      sink.onColumnData(new CompleteDataColumn(currentRowColIndex, null));
      return true;
    }

    // 2. Ensure we have the actual data bytes
    if (accumulator.remaining() < length) {
      return false; // Yield gracefully instead of throwing!
    }

    // 3. Extract and emit raw bytes
    byte[] data = new byte[length];
    accumulator.get(data);
    sink.onColumnData(new CompleteDataColumn(currentRowColIndex, data));

    return true;
  }

  private boolean parsePlpColumn(ColumnMeta colMeta) {
    // 1. Check for the initial 8-byte total length header if starting a new PLP column
    if (expectingPlpTotalLengthHeader) {
      if (accumulator.remaining() < 8) return false;

      long totalLength = accumulator.getLong();
      expectingPlpTotalLengthHeader = false;

      if (totalLength == -1L || totalLength == 0xFFFFFFFFFFFFFFFFL) {
        // PLP is Null
        sink.onColumnData(new CompleteDataColumn(currentRowColIndex, null));
        return true; // Column complete, move to next
      }
    }

    // 2. Drain as many chunks as possible from the current buffer
    while (true) {
      if (expectingPlpChunkLength || pendingPlpChunkBytes == 0) {
        if (accumulator.remaining() < 4) return false; // Need more data for the length header

        pendingPlpChunkBytes = accumulator.getInt();
        expectingPlpChunkLength = false;

        if (pendingPlpChunkBytes == 0) {
          // PLP Terminator reached.
          // Simply reset state and return true to advance to the next column.
          expectingPlpChunkLength = true;
          return true;
        }
      }

      int bytesToRead = (int) Math.min(accumulator.remaining(), pendingPlpChunkBytes);
      if (bytesToRead == 0) return false; // Need more data for the chunk payload

      byte[] chunkData = new byte[bytesToRead];
      accumulator.get(chunkData);
      pendingPlpChunkBytes -= bytesToRead;

      // Emit partial raw bytes immediately
      sink.onColumnData(new PartialDataColumn(currentRowColIndex, chunkData));

      if (pendingPlpChunkBytes == 0) {
        expectingPlpChunkLength = true; // Ready to read the next chunk length on the next iteration
      }
    }
  }

  private void ensureCapacity(int neededBytes) {
    if (accumulator.remaining() < neededBytes) {
      ByteBuffer expanded = ByteBuffer.allocate(accumulator.capacity() + neededBytes + 8192).order(ByteOrder.LITTLE_ENDIAN);
      accumulator.flip();
      expanded.put(accumulator);
      accumulator = expanded;
    }
  }

  private String generateCrashDump(ByteBuffer buffer, byte badToken) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n================ FATAL DECODER DESYNC =================\n");
    sb.append(String.format("Failed Token Type : 0x%02X\n", badToken));
    sb.append(String.format("Buffer Position   : %d\n", buffer.position()));
    sb.append(String.format("Buffer Limit      : %d\n", buffer.limit()));

    if (currentMetaData != null) {
      sb.append(String.format("Last Known Column : %d of %d\n",
          currentRowColIndex, currentMetaData.getColumns().size()));
    }

    sb.append("\n--- HEX DUMP (Surrounding Bytes) ---\n");

    // Look 16 bytes backwards (or to the start of the buffer)
    int start = Math.max(0, buffer.position() - 16);
    // Look 32 bytes forwards (or to the limit)
    int end = Math.min(buffer.limit(), buffer.position() + 32);

    for (int i = start; i < end; i++) {
      if (i == buffer.position() - 1) {
        // Highlight the byte that caused the crash
        sb.append(String.format("[0x%02X] ", buffer.get(i)));
      } else {
        sb.append(String.format("0x%02X ", buffer.get(i)));
      }
    }
    sb.append("\n=======================================================\n");
    return sb.toString();
  }
}