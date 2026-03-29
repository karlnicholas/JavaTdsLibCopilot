package org.tdslib.javatdslib.reactive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.PartialDataColumn;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.transport.ConnectionContext;

/**
 * Responsible for assembling a single row's data.
 * Upgraded to lay the tracks for deferred LOB handoffs.
 */
public class RowDrainer {
  private static final Logger logger = LoggerFactory.getLogger(RowDrainer.class);

  // ADDED: Marker for columns we haven't read from the network yet
  public static final Object UNFETCHED = new Object();

  private final ColMetaDataToken metaData;
  private final ConnectionContext context;
  private final Object[] assemblingRow; // Shifted from byte[][] to Object[]
  private final int totalColumns;

  private boolean isReadyToYield = false; // (Existing)
  private boolean isRowEmitted = false;   // NEW: Tracks if we've handed the row to the user
  private boolean isFullyComplete = false; // NEW: Tracks if the network stream for this row is 100% finished

  // ADD FIELD
  private final TdsTokenQueue tokenQueue;

  public RowDrainer(ColMetaDataToken metaData, ConnectionContext context, TdsTokenQueue tokenQueue) {
    this.metaData = metaData;
    this.context = context;
    this.tokenQueue = tokenQueue;
    this.totalColumns = metaData.getColumns().size();
    this.assemblingRow = new Object[totalColumns];
// Initialize the row to UNFETCHED
    java.util.Arrays.fill(this.assemblingRow, UNFETCHED);
  }

  /**
   * Phase B: The Asynchronous Drain
   * Called by AsyncWorkerSink every time a chunk arrives from the network.
   */
  public void processColumn(ColumnData cd) {
    int colIndex = cd.getColumnIndex();
    ColumnMeta colMeta = metaData.getColumns().get(colIndex);
    TdsType tdsType = TdsType.valueOf(colMeta.getDataType());

    // FIX: A column is PLP if the strategy is PLP, OR if the Framer chunked it as PartialData
    boolean isPlp = (cd instanceof PartialDataColumn) ||
        (tdsType != null && tdsType.strategy == TdsType.LengthStrategy.PLP);

    if (isPlp) {
      logger.trace("[RowDrainer] PLP/LOB detected at index {}. Yielding early.", colIndex);
      // FIX: Do not drop the chunk! Save it in the array for StatefulRow to process.
      assemblingRow[colIndex] = cd;
      isReadyToYield = true;
      return;
    }

    if (cd instanceof CompleteDataColumn c) {
      assemblingRow[colIndex] = c.getData(); // Standard materialized data
      checkRowCompletion(colIndex);
    }
  }

  private void checkRowCompletion(int justFinishedColIndex) {
    if (justFinishedColIndex == totalColumns - 1) {
      isReadyToYield = true;
      isFullyComplete = true; // NEW: Mark the absolute physical end of the row
    }
  }

  public boolean isReadyToYield() {
    return isReadyToYield;
  }

  public boolean isRowEmitted() {
    return isRowEmitted;
  }

  public void setRowEmitted(boolean rowEmitted) {
    this.isRowEmitted = rowEmitted;
  }

  public boolean isFullyComplete() {
    return isFullyComplete;
  }

  public StatefulRow assembleRow() {
    return new StatefulRow(this.assemblingRow, this.metaData, this.context, this.tokenQueue);
  }
}