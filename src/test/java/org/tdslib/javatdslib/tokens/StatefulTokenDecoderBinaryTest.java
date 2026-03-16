package org.tdslib.javatdslib.tokens;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.DefaultConnectionContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatefulTokenDecoderBinaryTest {

  private StatefulTokenDecoder decoder;
  private TrackingDecoderSink trackingSink;
  private ConnectionContext context;

  @BeforeEach
  void setUp() {
    TokenParserRegistry registry = TokenParserRegistry.DEFAULT;
    context = new DefaultConnectionContext();

    // Use our custom tracking sink instead of a Mockito mock
    trackingSink = new TrackingDecoderSink();

    decoder = new StatefulTokenDecoder(registry, context, trackingSink, new ArrayList<>());
  }

  @Test
  void testReconstructAndDecodeThreeSelectDump() throws Exception {
    String baseFileName = "src/test/resources/three select dump packet ";

    // Iterate through the 6 split packet files
    for (int i = 1; i <= 6; i++) {
      Path path = Paths.get(baseFileName + i + ".txt");
      byte[] packetBytes = parseWiresharkHexDump(path);

      ByteBuffer packet = ByteBuffer.wrap(packetBytes).order(ByteOrder.LITTLE_ENDIAN);

      // TDS Packet Header is 8 bytes. Advance the position to pass only the payload to the decoder.
      packet.position(8);
      ByteBuffer payloadOnly = packet.slice();

      boolean isLast = (i == 6);

      // Feed the payload into the decoder
      decoder.onPayloadAvailable(payloadOnly, isLast);
    }

    // 1. Print the chronological audit trail to the console for debugging
    System.out.println("=== DECODER EVENT AUDIT TRAIL ===");
    for (String event : trackingSink.getEvents()) {
      System.out.println(event);
    }
    System.out.println("=================================");

    // 2. Perform basic verifications based on the audit trail
    List<String> events = trackingSink.getEvents();
    assertFalse(events.isEmpty(), "Decoder should have emitted events");

    long doneTokenCount = events.stream().filter(e -> e.startsWith("TOKEN: DONE")).count();
    assertTrue(doneTokenCount >= 4, "Expected at least 4 DONE tokens for the 3 SELECT statements");

    boolean hasErrors = events.stream().anyMatch(e -> e.startsWith("ERROR"));
    assertFalse(hasErrors, "Decoder threw an exception during parsing");
  }

  /**
   * Custom Sink to chronologically track every emission from the decoder.
   */
  private static class TrackingDecoderSink implements TdsDecoderSink {
    private final List<String> events = new ArrayList<>();

    @Override
    public void onToken(Token token) {
      // Assuming Token has a getTokenType() method. If not, fallback to getClass().getSimpleName()
      String tokenName = token.getType() != null ? token.getType().name() : token.getClass().getSimpleName();
      events.add("TOKEN: " + tokenName);
    }

    @Override
    public void onColumnData(ColumnData data) {
      int colIndex = data.getColumnIndex();

      if (data instanceof CompleteDataColumn complete) {
        byte[] bytes = complete.getData();
        int len = bytes == null ? -1 : bytes.length;
        events.add(String.format("COLUMN: index=%d, type=CompleteDataColumn, length=%d", colIndex, len));

      } else if (data instanceof PartialDataColumn partial) {
        byte[] bytes = partial.getChunk();
        int len = bytes == null ? -1 : bytes.length;
        events.add(String.format("COLUMN: index=%d, type=PartialDataColumn, length=%d", colIndex, len));

      } else {
        events.add(String.format("COLUMN: index=%d, type=%s", colIndex, data.getClass().getSimpleName()));
      }
    }

    @Override
    public void onError(Throwable error) {
      events.add("ERROR: " + error.getMessage());
      error.printStackTrace();
    }

    public List<String> getEvents() {
      return events;
    }
  }

  // ... [parseWiresharkHexDump and isHexByte remain exactly the same as before] ...

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