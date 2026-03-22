package org.tdslib.javatdslib.reactive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.PartialDataColumn;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.io.ByteArrayOutputStream;

/**
 * Responsible for assembling a single row's data.
 * Currently materializes all columns (including PLPs) into memory.
 * Designed to be upgraded to a deferred LOB handoff architecture in the future.
 */
public class RowDrainer {
  private static final Logger logger = LoggerFactory.getLogger(RowDrainer.class);

  private final ColMetaDataToken metaData;
  private final ConnectionContext context;
  private final byte[][] assemblingRow;
  private final int totalColumns;

  private int activePlpIndex = -1;
  private final ByteArrayOutputStream plpAccumulator = new ByteArrayOutputStream();
  private boolean isComplete = false;

  public RowDrainer(ColMetaDataToken metaData, ConnectionContext context) {
    this.metaData = metaData;
    this.context = context;
    this.totalColumns = metaData.getColumns().size();
    this.assemblingRow = new byte[totalColumns][];
  }

  /**
   * Phase B: The Asynchronous Drain
   * Called by AsyncWorkerSink every time a chunk arrives from the network.
   */
  public void processColumn(ColumnData cd) {
    int colIndex = cd.getColumnIndex();
    flushPlpIfNecessary(colIndex);

    if (cd instanceof PartialDataColumn p) {
      activePlpIndex = colIndex;
      if (p.getChunk() != null) {
        plpAccumulator.write(p.getChunk(), 0, p.getChunk().length);
      }
    } else if (cd instanceof CompleteDataColumn c) {
      assemblingRow[colIndex] = c.getData();
      checkRowCompletion(colIndex);
    }
  }

  /**
   * Flushes accumulating PLP bytes into the row payload.
   * Can be triggered by moving to a new column, or forcefully by a Done/Row token.
   */
  public void flushPlpIfNecessary(int incomingColIndex) {
    if (activePlpIndex != -1 && activePlpIndex != incomingColIndex) {
      assemblingRow[activePlpIndex] = plpAccumulator.toByteArray();
      plpAccumulator.reset();
      int finishedIndex = activePlpIndex;
      activePlpIndex = -1;
      checkRowCompletion(finishedIndex);
    }
  }

  private void checkRowCompletion(int justFinishedColIndex) {
    if (justFinishedColIndex == totalColumns - 1) {
      this.isComplete = true;
    }
  }

  /**
   * Phase C Trigger: Informs the Sink if the row is ready to be yielded.
   */
  public boolean isComplete() {
    return isComplete;
  }

  /**
   * Phase C: Wraps the internal state into the StatefulRow for the user's synchronous get().
   */
  public StatefulRow assembleRow() {
    return new StatefulRow(this.assemblingRow, this.metaData, this.context);
  }
}