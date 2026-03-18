package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.models.*;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ReactiveResultAssemblerTest {

  private TdsTransport mockTransport;
  private ConnectionContext mockContext;
  private DataSink<Result.Segment> mockDownstreamSink;
  private ReactiveResultAssembler assembler;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockTransport = mock(TdsTransport.class);
    mockContext = mock(ConnectionContext.class);
    mockDownstreamSink = (DataSink<Result.Segment>) mock(DataSink.class);

    // Pass null as the 3rd argument (Executor) so the test runs synchronously
    assembler = new ReactiveResultAssembler(mockTransport, mockContext, null);
    assembler.setDownstreamSink(mockDownstreamSink);
  }

  @Test
  void testHighAndLowWatermarkThrottling() {
    // 1. Arrange: Setup prerequisite tokens to initialize the row array
    ColMetaDataToken mockMetaData = mock(ColMetaDataToken.class);
    List mockColumns = mock(List.class);
    when(mockMetaData.getColumns()).thenReturn(mockColumns);
    when(mockColumns.size()).thenReturn(1); // 1 column array

    // Create a payload just over the 5MB High Watermark
    byte[] massivePayload = new byte[5 * 1024 * 1024 + 10];
    CompleteDataColumn massiveColumn = new CompleteDataColumn(0, massivePayload);

    // 2. Act: Send the required sequence (Meta -> Row -> Column)
    assembler.onToken(mockMetaData);
    assembler.onToken(new RowToken(mockMetaData));
    assembler.onColumnData(massiveColumn);

    // 3. Assert 1: High watermark exceeded, network should be suspended.
    verify(mockTransport, times(1)).suspendNetworkRead();
    verify(mockTransport, never()).resumeNetworkRead();

    // 4. Act 2: Request the item (Worker Thread drain loop wakes up)
    assembler.request(1);

    // 5. Assert 2: Low watermark reached, network should be resumed.
    verify(mockTransport, times(1)).resumeNetworkRead();
  }

  @Test
  void testStandardRowAssemblyAndDemand() {
    // Arrange
    ColMetaDataToken mockMetaData = mock(ColMetaDataToken.class);
    List mockColumns = mock(List.class);
    when(mockMetaData.getColumns()).thenReturn(mockColumns);
    when(mockColumns.size()).thenReturn(2); // Row requires 2 columns to complete

    byte[] col0Data = new byte[]{1, 2, 3};
    byte[] col1Data = new byte[]{4, 5, 6};

    // Act: Push a complete row into the queue (Network Thread)
    assembler.onToken(mockMetaData);
    assembler.onToken(new RowToken(mockMetaData));
    assembler.onColumnData(new CompleteDataColumn(0, col0Data));
    assembler.onColumnData(new CompleteDataColumn(1, col1Data));

    // Ensure nothing emitted before demand
    verify(mockDownstreamSink, never()).pushNext(any());

    // Request 1 row (Worker Thread)
    assembler.request(1);

    // Assert
    ArgumentCaptor<Result.Segment> segmentCaptor = ArgumentCaptor.forClass(Result.Segment.class);
    verify(mockDownstreamSink, times(1)).pushNext(segmentCaptor.capture());

    // Verify the emitted segment is a RowSegment (StatefulRow)
    Result.Segment emittedSegment = segmentCaptor.getValue();
    assertTrue(emittedSegment instanceof Result.RowSegment);
  }

  @Test
  void testDoneTokenEmitsUpdateCountAndCompletes() {
    // Arrange
    DoneToken mockDone = mock(DoneToken.class);
    DoneStatus mockStatus = mock(DoneStatus.class);

    when(mockDone.getStatus()).thenReturn(mockStatus);
    when(mockStatus.hasCount()).thenReturn(true);
    when(mockDone.getCount()).thenReturn(42L);
    when(mockStatus.hasMoreResults()).thenReturn(false);

    // Act
    assembler.onToken(mockDone);

    // Request 1 to process the DoneToken and emit the UpdateCount segment
    assembler.request(1);

    // Assert
    ArgumentCaptor<Result.Segment> segmentCaptor = ArgumentCaptor.forClass(Result.Segment.class);
    verify(mockDownstreamSink, times(1)).pushNext(segmentCaptor.capture());
    verify(mockDownstreamSink, times(1)).pushComplete();

    assertTrue(segmentCaptor.getValue() instanceof TdsUpdateCount);
    assertEquals(42L, ((TdsUpdateCount) segmentCaptor.getValue()).value());
  }

  @Test
  void testErrorPropagation() {
    // Arrange
    RuntimeException testException = new RuntimeException("Network Failure");

    // Act
    assembler.onError(testException);

    // Errors are prioritized but still require a drain cycle to push downstream
    assembler.request(1);

    // Assert
    verify(mockDownstreamSink, times(1)).pushError(testException);
    verify(mockDownstreamSink, never()).pushComplete();
  }

  @Test
  void testCancellationClearsQueueAndCancelsTransport() {
    // Arrange
    assembler.onToken(mock(ColMetaDataToken.class));

    // Act
    assembler.cancel();

    // Attempt to request data after cancellation
    assembler.request(1);

    // Assert
    verify(mockTransport, times(1)).cancelCurrent();
    verify(mockDownstreamSink, never()).pushNext(any());
  }

  @Test
  void testDemandThrottlingRespectsRequestedAmount() {
    // Arrange
    ColMetaDataToken mockMetaData = mock(ColMetaDataToken.class);
    List mockColumns = mock(List.class);
    when(mockMetaData.getColumns()).thenReturn(mockColumns);
    when(mockColumns.size()).thenReturn(1); // Row requires 1 column to complete

    // Push 3 complete rows into the queue
    assembler.onToken(mockMetaData);
    for (int i = 0; i < 3; i++) {
      assembler.onToken(new RowToken(mockMetaData));
      assembler.onColumnData(new CompleteDataColumn(0, new byte[]{1}));
    }

    // Act & Assert 1: Request only 2 rows
    assembler.request(2);
    verify(mockDownstreamSink, times(2)).pushNext(any());

    // Act & Assert 2: Request the final row
    assembler.request(1);
    verify(mockDownstreamSink, times(3)).pushNext(any());
  }
}