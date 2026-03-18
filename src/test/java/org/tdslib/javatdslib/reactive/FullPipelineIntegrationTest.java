package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tdslib.javatdslib.tokens.StatefulTokenDecoder;
import org.tdslib.javatdslib.tokens.TokenParserRegistry;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.DefaultConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class FullPipelineIntegrationTest {

  private ExecutorService workerThreadPool;
  private TdsTokenQueue tokenQueue;
  private AsyncWorkerSink workerSink;
  private StatefulTokenDecoder decoder;

  @BeforeEach
  void setUp() {
    // 1. Thread Pool for R2DBC Assembly
    workerThreadPool = Executors.newSingleThreadExecutor(r -> new Thread(r, "R2DBC-Worker-Thread"));

    // 2. Mocks for dependencies we aren't testing here
    TdsTransport mockTransport = mock(TdsTransport.class);
    ConnectionContext mockContext = new DefaultConnectionContext();

    // 3. Assemble the Reactive Pipeline
    tokenQueue = new TdsTokenQueue(mockTransport);
    workerSink = new AsyncWorkerSink(tokenQueue, mockContext, workerThreadPool);

    // 4. Assemble the Protocol Decoder (Note: The queue IS the TdsDecoderSink)
    TokenParserRegistry registry = TokenParserRegistry.DEFAULT;
    decoder = new StatefulTokenDecoder(registry, mockContext, tokenQueue, new ArrayList<>());
  }

  @AfterEach
  void tearDown() {
    workerThreadPool.shutdownNow();
  }

  @Test
  void testEndToEndBinaryStreamDecodingAndAssembly() throws Exception {
    // 1. Open the demand valve so the worker thread freely drains whatever arrives
    System.out.println("[Main/Test-Thread] Initiating infinite demand to start stream...");
    workerSink.request(Long.MAX_VALUE);

    String baseFileName = "src/test/resources/three select dump packet ";
    Thread.currentThread().setName("NIO-Network-Thread");

    // 2. Feed the actual Wireshark TCP payloads into the decoder
    System.out.println("[NIO-Network-Thread] Pushing raw TCP packets into decoder...");

    for (int i = 1; i <= 6; i++) {
      Path path = Paths.get(baseFileName + i + ".txt");
      byte[] packetBytes = parseWiresharkHexDump(path);

      ByteBuffer packet = ByteBuffer.wrap(packetBytes).order(ByteOrder.LITTLE_ENDIAN);

      // TDS Packet Header is 8 bytes. Slice to pass only the payload.
      packet.position(8);
      ByteBuffer payloadOnly = packet.slice();

      boolean isLast = (i == 6);

      // This call executes on the NIO thread. The decoder chunks it, puts it in the TdsTokenQueue,
      // and immediately returns. The actual row assembly happens concurrently on the worker thread.
      decoder.onPayloadAvailable(payloadOnly, isLast);
    }

    System.out.println("[NIO-Network-Thread] Finished pushing raw bytes. Waiting for assembly...");

    // 3. Wait for the Worker Thread to finish draining the queue and assembling segments.
    // The final DONE token in packet 6 has no "More" flag, which will trigger pushComplete().
    workerSink.awaitCompletion();

// 4. Verification
    List<Result.Segment> segments = workerSink.getReceivedSegments();
    assertFalse(segments.isEmpty(), "Worker thread should have assembled segments.");

    // Filter for only the Row segments to inspect data
    List<StatefulRow> rows = segments.stream()
        .filter(segment -> segment instanceof StatefulRow)
        .map(segment -> (StatefulRow) segment)
        .toList();

    assertEquals(3, rows.size(), "Exactly 3 complete R2DBC Row segments should be assembled.");

    // The expected lengths for columns 0 through 30 based on the audit trail
    int[] expectedLengths = {
        4, 1, 1, 2, 4, 8, 5, 5, 4, 8, 4, 8, 3, 5, 8, 8, 4, 10, 9, 22, // Col 0-19
        5000,                                                         // Col 20 (Stitched)
        16, 20, 28,                                                   // Col 21-23
        8000,                                                         // Col 24 (Stitched)
        4, 4, 4, 4, 16,                                               // Col 25-29
        68                                                            // Col 30 (Stitched)
    };

    for (int i = 0; i < rows.size(); i++) {
      StatefulRow row = rows.get(i);
      byte[][] rowData = row.getRowData(); // Accesses the payload provided in StatefulRow.java

      assertEquals(expectedLengths.length, rowData.length,
          String.format("Row %d should have %d columns.", i, expectedLengths.length));

      for (int colIndex = 0; colIndex < expectedLengths.length; colIndex++) {
        byte[] actualColumn = rowData[colIndex];

        assertNotNull(actualColumn,
            String.format("Row %d, Column %d should not be null", i, colIndex));

        assertEquals(expectedLengths[colIndex], actualColumn.length,
            String.format("Row %d, Column %d length mismatch. Expected %d but got %d",
                i, colIndex, expectedLengths[colIndex], actualColumn.length));
      }
    }

    System.out.println("Integration Test Verified: 3 rows, 31 columns each, all PLP chunks correctly stitched.");
  }

  // ====================================================================================
  // HEX DUMP UTILITIES
  // ====================================================================================

  private byte[] parseWiresharkHexDump(Path filePath) throws IOException {
    StringBuilder hexCollector = new StringBuilder();
    List<String> lines = Files.readAllLines(filePath);

    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) continue;

      if (trimmed.startsWith("[")) {
        int bracketEnd = trimmed.indexOf(']');
        if (bracketEnd != -1) {
          trimmed = trimmed.substring(bracketEnd + 1).trim();
        }
      }

      String[] tokens = trimmed.split("\\s+");
      for (int i = 1; i < tokens.length; i++) {
        if (isHexByte(tokens[i])) {
          hexCollector.append(tokens[i]);
        } else {
          break;
        }
      }
    }
    return HexFormat.of().parseHex(hexCollector.toString());
  }

  private boolean isHexByte(String token) {
    if (token.length() != 2) return false;
    return Character.digit(token.charAt(0), 16) >= 0 && Character.digit(token.charAt(1), 16) >= 0;
  }
}