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
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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

  private Scheduler workerScheduler;
  private TdsTokenQueue tokenQueue;
  private AsyncWorkerSink workerSink;
  private StatefulTokenDecoder decoder;
  private ConnectionContext mockContext;

  @BeforeEach
  void setUp() {
    workerScheduler = Schedulers.newSingle("R2DBC-Worker-Thread");
    TdsTransport mockTransport = mock(TdsTransport.class);
    mockContext = new DefaultConnectionContext();

    tokenQueue = new TdsTokenQueue(mockTransport);
    workerSink = new AsyncWorkerSink(tokenQueue, mockContext, workerScheduler);

    TokenParserRegistry registry = TokenParserRegistry.DEFAULT;
    decoder = new StatefulTokenDecoder(registry, mockContext, tokenQueue, new ArrayList<>());
  }

  @AfterEach
  void tearDown() {
    workerScheduler.dispose(); // Safely clean up Reactor's thread
  }

  @Test
  void testThreeSelectDump() throws Exception {
    runIntegrationTest("src/test/resources/three select dump packet ");
  }

  @Test
  void testSelectAllDump() throws Exception {
    runIntegrationTest("src/test/resources/select all dump packet ");
  }

  /**
   * Common test runner that pumps raw TDS packets through the decoder and verifies
   * the final assembled R2DBC segments on the worker thread.
   */
  private void runIntegrationTest(String filePrefix) throws Exception {
    System.out.println("[Main/Test-Thread] Testing pipeline with: " + filePrefix);

    // 1. Initiate demand
    workerSink.request(Long.MAX_VALUE);

    String originalThreadName = Thread.currentThread().getName();
    Thread.currentThread().setName("NIO-Network-Thread");

    try {
      // 2. Feed the 6 binary packet files into the decoder
      for (int i = 1; i <= 6; i++) {
        Path path = Paths.get(filePrefix + i + ".txt");
        byte[] packetBytes = parseWiresharkHexDump(path);

        ByteBuffer packet = ByteBuffer.wrap(packetBytes).order(ByteOrder.LITTLE_ENDIAN);

        // TDS Packet Header is 8 bytes. Skip to payload.
        packet.position(8);
        ByteBuffer payloadOnly = packet.slice();

        decoder.onPayloadAvailable(payloadOnly, (i == 6));
      }

      System.out.println("[NIO-Network-Thread] Packets pushed. Waiting for worker assembly...");

      // 3. Wait for Worker Thread completion
      workerSink.awaitCompletion();

      // 4. Verification Logic
      verifyAssembledData(filePrefix);

    } finally {
      Thread.currentThread().setName(originalThreadName);
    }
  }

  private void verifyAssembledData(String scenarioName) {
    List<Result.Segment> segments = workerSink.getReceivedSegments();
    assertFalse(segments.isEmpty(), "Worker thread should have assembled segments for " + scenarioName);

    List<StatefulRow> rows = segments.stream()
        .filter(segment -> segment instanceof StatefulRow)
        .map(segment -> (StatefulRow) segment)
        .toList();

    assertEquals(3, rows.size(), "Expected 3 rows for " + scenarioName);

    // Expected column lengths based on the established audit trail
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
      byte[][] rowData = row.getRowData();

      assertEquals(expectedLengths.length, rowData.length,
          String.format("[%s] Row %d: column count mismatch.", scenarioName, i));

      for (int colIndex = 0; colIndex < expectedLengths.length; colIndex++) {
        byte[] actualColumn = rowData[colIndex];
        assertNotNull(actualColumn, String.format("[%s] Row %d, Col %d is null", scenarioName, i, colIndex));
        assertEquals(expectedLengths[colIndex], actualColumn.length,
            String.format("[%s] Row %d, Col %d length mismatch.", scenarioName, i, colIndex));
      }
    }

    System.out.println("Verified " + scenarioName + ": 3 rows, 31 columns, all PLP stitched correctly.");
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