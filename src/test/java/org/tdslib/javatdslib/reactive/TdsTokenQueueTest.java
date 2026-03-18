package org.tdslib.javatdslib.reactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tdslib.javatdslib.reactive.events.ErrorEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.transport.TdsTransport;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TdsTokenQueueTest {

  private TdsTransport mockTransport;
  private TdsTokenQueue queue;
  private Runnable mockCallback;

  @BeforeEach
  void setUp() {
    mockTransport = mock(TdsTransport.class);
    queue = new TdsTokenQueue(mockTransport);

    // We mock the callback so we can verify the queue attempts to wake up the consumer
    mockCallback = mock(Runnable.class);
    queue.setOnEventAvailableCallback(mockCallback);
  }

  @Test
  void testHighAndLowWatermarkThrottling() {
    // Arrange: Create a payload just over the 5MB High Watermark
    byte[] massivePayload = new byte[5 * 1024 * 1024 + 10];
    CompleteDataColumn massiveColumn = new CompleteDataColumn(0, massivePayload);

    // Act 1: Push the massive column onto the queue
    queue.onColumnData(massiveColumn);

    // Assert 1: High watermark exceeded. Transport suspended, consumer notified.
    verify(mockTransport, times(1)).suspendNetworkRead();
    verify(mockTransport, never()).resumeNetworkRead();
    verify(mockCallback, times(1)).run();

    // Act 2: Consumer polls the item off the queue
    TdsStreamEvent event = queue.poll();

    // Assert 2: Low watermark reached. Transport resumed.
    assertNotNull(event);
    verify(mockTransport, times(1)).resumeNetworkRead();
  }

  @Test
  void testErrorPropagation() {
    // Arrange
    RuntimeException testException = new RuntimeException("Network Failure");

    // Act
    queue.onError(testException);

    // Assert
    verify(mockCallback, times(1)).run(); // Consumer must be notified of the error

    TdsStreamEvent event = queue.poll();
    assertTrue(event instanceof ErrorEvent);
    assertEquals(testException, ((ErrorEvent) event).error());
  }

  @Test
  void testClearQueue() {
    // Arrange: Put something in the queue
    queue.onToken(mock(ColMetaDataToken.class));
    assertNotNull(queue.poll());

    // Act: Put another item, then immediately clear
    queue.onToken(mock(ColMetaDataToken.class));
    queue.clear();

    // Assert: Queue is empty
    assertNull(queue.poll());
  }
}