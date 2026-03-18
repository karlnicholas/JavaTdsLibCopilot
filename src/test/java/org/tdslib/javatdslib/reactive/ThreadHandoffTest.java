package org.tdslib.javatdslib.reactive;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.tokens.models.DoneStatus;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ThreadHandoffTest {

  // Swap ExecutorService for Reactor Scheduler
  private Scheduler workerScheduler;
  private TdsTokenQueue tokenQueue;
  private AsyncWorkerSink workerSink;
  private ColMetaDataToken mockMetaData;

  @BeforeEach
  void setUp() {
    // Let Reactor manage the thread lifecycle
    workerScheduler = Schedulers.newSingle("R2DBC-Worker-Thread");

    TdsTransport mockTransport = mock(TdsTransport.class);
    ConnectionContext mockContext = mock(ConnectionContext.class);

    tokenQueue = new TdsTokenQueue(mockTransport);

    // Pass the scheduler to the sink
    workerSink = new AsyncWorkerSink(tokenQueue, mockContext, workerScheduler);

    // FIX: Use a real list with a mock column instead of a mocked list
    mockMetaData = mock(ColMetaDataToken.class);
    ColumnMeta mockCol = mock(ColumnMeta.class);
    when(mockMetaData.getColumns()).thenReturn(java.util.Collections.singletonList(mockCol));
  }

  @AfterEach
  void tearDown() {
    // Safely dispose of the Reactor thread
    workerScheduler.dispose();
  }

  @Test
  void testSingleResultSetMultipleRows() throws InterruptedException {
    int numRows = 100;

    System.out.println("[Main/Test-Thread] Requesting initial demand...");
    // Request an arbitrarily large number of items so the worker freely drains the queue
    workerSink.request(Long.MAX_VALUE);

    System.out.println("[Main/Test-Thread] Pushing tokens to queue (Simulating NIO)...");
    Thread.currentThread().setName("NIO-Network-Thread");

    DoneToken batchDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x01), 0xBE, 0);
    DoneToken finalDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x010), 0xC1, numRows);

    tokenQueue.onToken(batchDone);
    tokenQueue.onToken(mockMetaData);

    for (int i = 0; i < numRows; i++) {
      tokenQueue.onToken(new RowToken(mockMetaData));
      tokenQueue.onColumnData(new CompleteDataColumn(0, new byte[]{ (byte) i }));
    }

    tokenQueue.onToken(finalDone);

    verifyAndAwait(numRows);
  }

  @Test
  void testMultipleResultSetsSingleRowEach() throws InterruptedException {
    int numResultSets = 100;

    System.out.println("[Main/Test-Thread] Requesting initial demand...");
    workerSink.request(Long.MAX_VALUE);

    System.out.println("[Main/Test-Thread] Pushing tokens to queue (Simulating NIO)...");
    Thread.currentThread().setName("NIO-Network-Thread");

    DoneToken batchDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x01), 0xBE, 0);
    DoneToken moreResultsDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x011), 0xC1, 1);
    DoneToken finalDone = new DoneToken(TokenType.DONE.getValue(), new DoneStatus(0x010), 0xC1, 1);

    tokenQueue.onToken(batchDone);

    for (int i = 0; i < numResultSets; i++) {
      tokenQueue.onToken(mockMetaData);
      tokenQueue.onToken(new RowToken(mockMetaData));
      tokenQueue.onColumnData(new CompleteDataColumn(0, new byte[]{ (byte) i }));

      if (i < numResultSets - 1) {
        tokenQueue.onToken(moreResultsDone);
      } else {
        tokenQueue.onToken(finalDone);
      }
    }

    verifyAndAwait(numResultSets);
  }

  private void verifyAndAwait(int expectedRows) throws InterruptedException {
    System.out.println("[NIO-Network-Thread] Finished pushing to queue. Back to listening to socket.");

    workerSink.awaitCompletion();

    long rowCount = workerSink.getReceivedSegments().stream()
        .filter(segment -> segment instanceof StatefulRow)
        .count();

    assertEquals(expectedRows, rowCount, expectedRows + " row segments should be assembled on the worker thread.");
  }
}