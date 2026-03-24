package org.tdslib.javatdslib.tokens;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class StatefulTokenDecoderTest {
//
//  private TokenParserRegistry mockRegistry;
//  private TokenParser mockParser;
//  private ConnectionContext mockContext;
//  private TdsDecoderSink mockSink;
//  private StatefulTokenDecoder decoder;
//
//  @BeforeEach
//  void setUp() {
//    mockRegistry = mock(TokenParserRegistry.class);
//    mockParser = mock(TokenParser.class);
//    mockContext = mock(ConnectionContext.class);
//    mockSink = mock(TdsDecoderSink.class);
//
//    // Default: return the mock parser for any token type requested
//    when(mockRegistry.getParser(anyByte())).thenReturn(mockParser);
//
//    List<List<TdsParameter>> executions = Collections.emptyList();
//
//    decoder = new StatefulTokenDecoder(mockRegistry, mockContext, mockSink, executions);
//  }
//
//  @Test
//  void testStandardTokenParsing() {
//    // Arrange
//    byte tokenType = (byte) 0xFD; // DONE Token
//    Token mockDoneToken = mock(Token.class);
//    when(mockParser.parse(any(ByteBuffer.class), eq(tokenType), eq(mockContext))).thenReturn(mockDoneToken);
//
//    ByteBuffer payload = ByteBuffer.wrap(new byte[]{ tokenType, 0x00, 0x00 }); // Token + dummy payload
//
//    // Act
//    decoder.onPayloadAvailable(payload, true);
//
//    // Assert
//    verify(mockSink, times(1)).onToken(mockDoneToken);
//    verify(mockSink, never()).onError(any());
//  }
//
////  @Test
////  void testColMetaDataUpdatesStateAndTransitionsToRowParsing() {
////    // Arrange
////    byte metaType = (byte) 0x81; // COLMETADATA
////    byte rowType = (byte) 0xD1;  // ROW
////
////    // Create a mock metadata token indicating 2 columns
////    ColMetaDataToken mockMetaToken = mock(ColMetaDataToken.class);
////    ColumnMeta mockCol1 = mock(ColumnMeta.class);
////    ColumnMeta mockCol2 = mock(ColumnMeta.class);
////    when(mockMetaToken.getColumns()).thenReturn(List.of(mockCol1, mockCol2));
////
////    when(mockParser.parse(any(ByteBuffer.class), eq(metaType), eq(mockContext))).thenReturn(mockMetaToken);
////
////    // Payload: [MetaToken] -> [RowToken] -> [Col 1 Data (4 bytes)] -> [Col 2 Data (4 bytes)]
////    ByteBuffer payload = ByteBuffer.allocate(10);
////    payload.put(metaType);
////    payload.put(rowType);
////    payload.put(new byte[]{ 1, 2, 3, 4 }); // Mocked category 1 (fixed 4 bytes)
////    payload.put(new byte[]{ 5, 6, 7, 8 }); // Mocked category 1 (fixed 4 bytes)
////    payload.flip();
////
////    // Act
////    decoder.onPayloadAvailable(payload, true);
////
////    // Assert
////    verify(mockSink, times(1)).onToken(mockMetaToken);
////
////    // Capture the emitted columns
////    ArgumentCaptor<ColumnData> colCaptor = ArgumentCaptor.forClass(ColumnData.class);
////    verify(mockSink, times(2)).onColumnData(colCaptor.capture());
////
////    List<ColumnData> emittedColumns = colCaptor.getAllValues();
////    assertEquals(0, emittedColumns.get(0).getColumnIndex());
////    assertArrayEquals(new byte[]{ 1, 2, 3, 4 }, ((CompleteDataColumn) emittedColumns.get(0)).getData());
////
////    assertEquals(1, emittedColumns.get(1).getColumnIndex());
////    assertArrayEquals(new byte[]{ 5, 6, 7, 8 }, ((CompleteDataColumn) emittedColumns.get(1)).getData());
////  }
////
////  @Test
////  void testNetworkFragmentationHandlesPartialTokens() {
////    // Arrange
////    byte metaType = (byte) 0x81;
////    byte rowType = (byte) 0xD1;
////
////    ColMetaDataToken mockMetaToken = mock(ColMetaDataToken.class);
////    when(mockMetaToken.getColumns()).thenReturn(List.of(mock(ColumnMeta.class)));
////    when(mockParser.parse(any(ByteBuffer.class), eq(metaType), eq(mockContext))).thenReturn(mockMetaToken);
////
////    // Act 1: Send Metadata and Row Token, but only 2 bytes of the 4-byte column
////    ByteBuffer chunk1 = ByteBuffer.allocate(4);
////    chunk1.put(metaType);
////    chunk1.put(rowType);
////    chunk1.put(new byte[]{ 9, 9 }); // Incomplete column data
////    chunk1.flip();
////
////    decoder.onPayloadAvailable(chunk1, false);
////
////    // Assert 1: Token should emit, but NO column data should emit yet due to BufferUnderflow
////    verify(mockSink, times(1)).onToken(mockMetaToken);
////    verify(mockSink, never()).onColumnData(any());
////
////    // Act 2: Send the remaining 2 bytes
////    ByteBuffer chunk2 = ByteBuffer.allocate(2);
////    chunk2.put(new byte[]{ 9, 9 });
////    chunk2.flip();
////
////    decoder.onPayloadAvailable(chunk2, true);
////
////    // Assert 2: Column should now be fully assembled and emitted
////    ArgumentCaptor<ColumnData> colCaptor = ArgumentCaptor.forClass(ColumnData.class);
////    verify(mockSink, times(1)).onColumnData(colCaptor.capture());
////
////    CompleteDataColumn colData = (CompleteDataColumn) colCaptor.getValue();
////    assertArrayEquals(new byte[]{ 9, 9, 9, 9 }, colData.getData());
////  }
//
////  @Test
////  void testTokenFragmentationStateLeakBug() {
////    // 1. OVERRIDE THE TRAP: Restrict the registry so it only knows about 0xFD.
////    // If the state leaks and it tries to parse the 0x00 byte, it will return null and correctly crash.
////    reset(mockRegistry);
////    when(mockRegistry.getParser(eq((byte) 0xFD))).thenReturn(mockParser);
////
////    // Arrange: The exact byte sequence from the crash dump (13 bytes total)
////    // 0xFD (DONE), 0x10 0x00 (Status), 0xC1 0x00 (Cmd), 0x03 0x00 0x00 0x00 0x00 0x00 0x00 0x00 (RowCount)
////    byte[] fullToken = new byte[] {
////        (byte)0xFD, 0x10, 0x00, (byte)0xC1, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
////    };
////
////    // Mock the DoneToken parser to consume exactly 12 bytes of payload (throwing Underflow if less)
////    Token mockDoneToken = mock(Token.class);
////    when(mockParser.parse(any(ByteBuffer.class), eq((byte)0xFD), eq(mockContext))).thenAnswer(invocation -> {
////      ByteBuffer buf = invocation.getArgument(0);
////      if (buf.remaining() < 12) throw new BufferUnderflowException();
////      buf.position(buf.position() + 12); // Consume payload
////      return mockDoneToken;
////    });
////
////    // Act 1: Send the first 12 bytes (Simulating network fragmentation, missing the last 0x00 byte)
////    ByteBuffer packet1 = ByteBuffer.allocate(12);
////    packet1.put(fullToken, 0, 12);
////    packet1.flip();
////    decoder.onPayloadAvailable(packet1, false);
////
////    // Act 2: Send the final 1 byte in a new packet
////    ByteBuffer packet2 = ByteBuffer.allocate(1);
////    packet2.put(fullToken, 12, 1);
////    packet2.flip();
////
////    decoder.onPayloadAvailable(packet2, true);
////
////    // ASSERTION 1: Verify the token was eventually emitted successfully
////    verify(mockSink, times(1)).onToken(mockDoneToken);
////
////    // ASSERTION 2: THE SMOKING GUN.
////    // If the bug is present, the decoder will swallow the IllegalStateException
////    // and push it to the sink. This verify will fail if the fix is missing!
////    verify(mockSink, never()).onError(any());
////  }
//
//  @Test
//  void testThrowsExceptionIfRowArrivesBeforeMetadata() {
//    // Arrange
//    byte rowType = (byte) 0xD1; // ROW
//    ByteBuffer payload = ByteBuffer.wrap(new byte[]{ rowType });
//
//    // Act
//    decoder.onPayloadAvailable(payload, true);
//
//    // Assert
//    ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
//    verify(mockSink, times(1)).onError(errorCaptor.capture());
//    assertTrue(errorCaptor.getValue() instanceof IllegalStateException);
//    assertEquals("Received ROW token before ColMetaData.", errorCaptor.getValue().getMessage());
//  }
}