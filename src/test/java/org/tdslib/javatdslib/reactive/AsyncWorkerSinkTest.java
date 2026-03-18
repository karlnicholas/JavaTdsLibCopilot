package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.TokenEvent;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.DoneStatus;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class AsyncWorkerSinkTest {

  private TdsEventPublisher mockPublisher;
  private ConnectionContext mockContext;
  private AsyncWorkerSink sink;

  @BeforeEach
  void setUp() {
    mockPublisher = mock(TdsEventPublisher.class);
    mockContext = mock(ConnectionContext.class);
    sink = new AsyncWorkerSink(mockPublisher, mockContext);
  }

  @Test
  void testStandardRowAssembly() {
    // Arrange: Setup Metadata for a 2-column row
    ColMetaDataToken mockMetaData = mock(ColMetaDataToken.class);
    List mockColumns = mock(List.class);
    when(mockMetaData.getColumns()).thenReturn(mockColumns);
    when(mockColumns.size()).thenReturn(2);

    // Act: Push the exact sequence of events the StatefulTokenDecoder would generate
    sink.pushNext(new TokenEvent(mockMetaData));
    sink.pushNext(new TokenEvent(new RowToken(mockMetaData)));
    sink.pushNext(new ColumnEvent(new CompleteDataColumn(0, new byte[]{1})));

    // Row is not finished yet, list should be empty
    assertTrue(sink.getReceivedSegments().isEmpty());

    // Push the final column to trigger checkRowCompletion()
    sink.pushNext(new ColumnEvent(new CompleteDataColumn(1, new byte[]{2})));

    // Assert: Row is complete and was added to the segments list
    assertEquals(1, sink.getReceivedSegments().size());
    assertTrue(sink.getReceivedSegments().get(0) instanceof StatefulRow);

    // Verify it correctly requested the next items
    verify(mockPublisher, times(4)).request(1);
  }

  @Test
  void testDoneTokenEmitsUpdateCount() {
    // Arrange
    DoneToken mockDone = mock(DoneToken.class);
    DoneStatus mockStatus = mock(DoneStatus.class);
    when(mockDone.getStatus()).thenReturn(mockStatus);
    when(mockStatus.hasCount()).thenReturn(true);
    when(mockDone.getCount()).thenReturn(42L);
    when(mockStatus.hasMoreResults()).thenReturn(true); // Don't trigger complete yet

    // Act
    sink.pushNext(new TokenEvent(mockDone));

    // Assert
    assertEquals(1, sink.getReceivedSegments().size());
    assertTrue(sink.getReceivedSegments().get(0) instanceof TdsUpdateCount);
    assertEquals(42L, ((TdsUpdateCount) sink.getReceivedSegments().get(0)).value());
  }
}