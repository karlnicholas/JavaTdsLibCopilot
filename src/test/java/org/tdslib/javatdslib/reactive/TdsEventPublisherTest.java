package org.tdslib.javatdslib.reactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.ErrorEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.reactive.events.TokenEvent;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class TdsEventPublisherTest {

  private TdsTransport mockTransport;
  private ConnectionContext mockContext;
  private DataSink<TdsStreamEvent> mockDownstreamSink;
  private TdsEventPublisher publisher;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockTransport = mock(TdsTransport.class);
    mockContext = mock(ConnectionContext.class);
    mockDownstreamSink = (DataSink<TdsStreamEvent>) mock(DataSink.class);

    // Pass null as the 3rd argument (Executor) so the test runs synchronously
    publisher = new TdsEventPublisher(mockTransport, mockContext, null);
    publisher.setDownstreamSink(mockDownstreamSink);
  }

  @Test
  void testHighAndLowWatermarkThrottling() {
    // Arrange: Create a payload just over the 5MB High Watermark
    byte[] massivePayload = new byte[5 * 1024 * 1024 + 10];
    CompleteDataColumn massiveColumn = new CompleteDataColumn(0, massivePayload);

    // Act 1: Send the massive column. The network thread handles this immediately.
    publisher.onColumnData(massiveColumn);

    // Assert 1: High watermark exceeded, network should be suspended.
    verify(mockTransport, times(1)).suspendNetworkRead();
    verify(mockTransport, never()).resumeNetworkRead();

    // Act 2: Request the raw event (Worker Thread drain loop wakes up)
    publisher.request(1);

    // Assert 2: Low watermark reached, network should be resumed.
    verify(mockTransport, times(1)).resumeNetworkRead();
  }

  @Test
  void testErrorPropagation() {
    // Arrange
    RuntimeException testException = new RuntimeException("Network Failure");

    // Act
    publisher.onError(testException);

    // Errors are prioritized but still require a drain cycle to push downstream
    publisher.request(1);

    // Assert
    verify(mockDownstreamSink, times(1)).pushError(testException);
  }

  @Test
  void testCancellationClearsQueueAndCancelsTransport() {
    // Arrange
    publisher.onToken(mock(ColMetaDataToken.class));

    // Act
    publisher.cancel();

    // Attempt to request data after cancellation
    publisher.request(1);

    // Assert
    verify(mockTransport, times(1)).cancelCurrent();
    verify(mockDownstreamSink, never()).pushNext(any());
  }

  @Test
  void testDemandThrottlingRespectsRequestedAmountOfEvents() {
    // Push 5 distinct events into the queue
    for (int i = 0; i < 5; i++) {
      publisher.onToken(mock(ColMetaDataToken.class));
    }

    // Act & Assert 1: Request only 2 events
    publisher.request(2);
    verify(mockDownstreamSink, times(2)).pushNext(any(TokenEvent.class));

    // Act & Assert 2: Request the final 3 events
    publisher.request(3);
    verify(mockDownstreamSink, times(5)).pushNext(any(TokenEvent.class));
  }
}