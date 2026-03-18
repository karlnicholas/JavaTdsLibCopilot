package org.tdslib.javatdslib.reactive;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.DoneStatus;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ThreadHandoffTest {

  private ExecutorService workerThreadPool;
  private TdsEventPublisher publisher;
  private AsyncWorkerSink workerSink;
  private ColMetaDataToken mockMetaData;

  @BeforeEach
  void setUp() {
    // 1. Create a dedicated worker thread, simulating the R2DBC execution context
    workerThreadPool = Executors.newSingleThreadExecutor(r -> new Thread(r, "R2DBC-Worker-Thread"));

    // 2. Setup Mocks
    TdsTransport mockTransport = mock(TdsTransport.class);
    ConnectionContext mockContext = mock(ConnectionContext.class);

    // 3. Initialize the decoupled architecture
    publisher = new TdsEventPublisher(mockTransport, mockContext, workerThreadPool);
    workerSink = new AsyncWorkerSink(publisher, mockContext);
    publisher.setDownstreamSink(workerSink);

    // 4. Mock Metadata (1 column per row)
    mockMetaData = mock(ColMetaDataToken.class);
    List mockColumns = mock(List.class);
    when(mockMetaData.getColumns()).thenReturn(mockColumns);
    when(mockColumns.size()).thenReturn(1);
  }

  @AfterEach
  void tearDown() {
    workerThreadPool.shutdownNow();
  }

  /**
   * Scenario 1: A single ColumnMetaData token followed by multiple Row tokens,
   * concluding with a single Done token (0x10) containing the total row count.
   */
  @Test
  void testSingleResultSetMultipleRows() throws InterruptedException {
    int numRows = 100;

    System.out.println("[Main/Test-Thread] Requesting initial demand...");
    publisher.request(1);

    System.out.println("[Main/Test-Thread] Pushing tokens to queue (Simulating NIO)...");
    Thread.currentThread().setName("NIO-Network-Thread");

    // DONE token before results (Status 0x01: More)
    DoneToken batchDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x01), 0xBE, 0);
    // Final DONE token (Status 0x10: Row count valid)
    DoneToken finalDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x010), 0xC1, numRows);

    publisher.onToken(batchDone);
    publisher.onToken(mockMetaData);

    // Push 100 rows consecutively under the same metadata
    for (int i = 0; i < numRows; i++) {
      publisher.onToken(new RowToken(mockMetaData));
      publisher.onColumnData(new CompleteDataColumn(0, new byte[]{ (byte) i }));
    }

    // Terminate stream
    publisher.onToken(finalDone);

    verifyAndAwait(numRows);
  }

  /**
   * Scenario 2: Multiple consecutive result sets, each containing its own ColumnMetaData,
   * a single Row, and a Done token (0x11) indicating more results to follow. The final
   * result set ends with a Done token (0x10) indicating completion.
   */
  @Test
  void testMultipleResultSetsSingleRowEach() throws InterruptedException {
    int numResultSets = 100;

    System.out.println("[Main/Test-Thread] Requesting initial demand...");
    publisher.request(1);

    System.out.println("[Main/Test-Thread] Pushing tokens to queue (Simulating NIO)...");
    Thread.currentThread().setName("NIO-Network-Thread");

    // DONE token before results (Status 0x01: More)
    DoneToken batchDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x01), 0xBE, 0);
    // DONE token mid-stream (Status 0x11: More, Row count valid)
    DoneToken moreResultsDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x011), 0xC1, 1);
    // Final DONE token (Status 0x10: Row count valid)
    DoneToken finalDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x010), 0xC1, 1);

    publisher.onToken(batchDone);

    // Push 100 distinct result sets
    for (int i = 0; i < numResultSets; i++) {
      publisher.onToken(mockMetaData);
      publisher.onToken(new RowToken(mockMetaData));
      publisher.onColumnData(new CompleteDataColumn(0, new byte[]{ (byte) i }));

      // Determine if this is a mid-stream DONE or the final terminating DONE
      if (i < numResultSets - 1) {
        publisher.onToken(moreResultsDone);
      } else {
        publisher.onToken(finalDone); // Triggers pushComplete()
      }
    }

    verifyAndAwait(numResultSets);
  }

  /**
   * Common helper to wait for worker thread completion and verify row counts.
   */
  private void verifyAndAwait(int expectedRows) throws InterruptedException {
    System.out.println("[NIO-Network-Thread] Finished pushing to queue. Back to listening to socket.");

    // Wait for the Worker Thread to finish draining the queue and assembling segments
    workerSink.awaitCompletion();

    // Verify exactly 'expectedRows' were successfully assembled
    long rowCount = workerSink.getReceivedSegments().stream()
        .filter(segment -> segment instanceof StatefulRow)
        .count();

    assertEquals(expectedRows, rowCount, expectedRows + " row segments should be assembled on the worker thread.");
  }
}