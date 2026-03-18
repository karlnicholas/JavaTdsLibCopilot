package org.tdslib.javatdslib.reactive;

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

  private TdsTokenQueue mockQueue;
  private ConnectionContext mockContext;
  private AsyncWorkerSink sink;

  @BeforeEach
  void setUp() {
    mockQueue = mock(TdsTokenQueue.class);
    mockContext = mock(ConnectionContext.class);

    // Pass null for the Executor so the drain loop runs synchronously on the main thread for testing
    sink = new AsyncWorkerSink(mockQueue, mockContext, null);
  }

  @Test
  void testStandardRowAssembly() {
    // Arrange: Setup Metadata for a 2-column row
    ColMetaDataToken mockMetaData = mock(ColMetaDataToken.class);
    List mockColumns = mock(List.class);
    when(mockMetaData.getColumns()).thenReturn(mockColumns);
    when(mockColumns.size()).thenReturn(2);

// Prime the mocked queue to return exactly one complete row sequence, then return null (empty)
    when(mockQueue.poll()).thenReturn(
        new TokenEvent(mockMetaData),
        new TokenEvent(new RowToken(mockMetaData)),
        new ColumnEvent(new CompleteDataColumn(0, new byte[]{1})),
        new ColumnEvent(new CompleteDataColumn(1, new byte[]{2})),
        (org.tdslib.javatdslib.reactive.events.TdsStreamEvent) null // <-- Cast here
    );

    // Act: Request enough items to pull all elements and trigger the assembly
    sink.request(4);

    // Assert: Row is complete and was added to the segments list
    assertEquals(1, sink.getReceivedSegments().size());
    assertTrue(sink.getReceivedSegments().get(0) instanceof StatefulRow);
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

// Prime the mocked queue
    when(mockQueue.poll()).thenReturn(
        new TokenEvent(mockDone),
        (org.tdslib.javatdslib.reactive.events.TdsStreamEvent) null // <-- Cast here
    );

    // Act
    sink.request(1);

    // Assert
    assertEquals(1, sink.getReceivedSegments().size());
    assertTrue(sink.getReceivedSegments().get(0) instanceof TdsUpdateCount);
    assertEquals(42L, ((TdsUpdateCount) sink.getReceivedSegments().get(0)).value());
  }
}