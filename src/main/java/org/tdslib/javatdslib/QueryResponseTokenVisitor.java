package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.info.InfoToken;
import org.tdslib.javatdslib.tokens.row.RowToken;

import java.util.ArrayList;
import java.util.List;

/**
 * Stateful visitor that collects the results of one or more result-sets
 * from a SQL batch execution.
 */
public class QueryResponseTokenVisitor implements TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(QueryResponseTokenVisitor.class);

  private final EnvChangeTokenVisitor envChangeVisitor;

  // ------------------- State -------------------
  private ColMetaDataToken currentMetadata;          // last seen COL_METADATA
  private final List<ResultSet> resultSets = new ArrayList<>();  // one per result set
  private ResultSet currentResultSet;                // currently building
  private boolean hasMoreResultSets = false;
  private boolean hasError = false;
  // ------------------------------------------------

  /**
   * Create a new QueryResponseTokenVisitor that will apply ENVCHANGE tokens to
   * the provided ConnectionContext and collect result sets produced by tokens.
   *
   * @param connectionContext connection context used for ENVCHANGE handling
   */
  public QueryResponseTokenVisitor(ConnectionContext connectionContext) {
    this.envChangeVisitor = new EnvChangeTokenVisitor(connectionContext);
    startNewResultSet();
  }

  private void startNewResultSet() {
    currentResultSet = new ResultSet();
    resultSets.add(currentResultSet);
  }

  @Override
  public void onToken(Token token) {
    switch (token.getType()) {
      case COL_METADATA:
        currentMetadata = (ColMetaDataToken) token;
        currentResultSet.setColumnCount(currentMetadata.getColumnCount());
        currentResultSet.setColumns(currentMetadata.getColumns());  // assuming getColumns() exists
        logger.info("New result set with {} columns", currentResultSet.getColumnCount());
        break;

      case ROW:
        if (currentMetadata == null) {
          logger.warn("ROW token without preceding COL_METADATA - ignoring");
          return;
        }
        RowToken row = (RowToken) token;
        currentResultSet.addRawRow(row.getColumnData());
        logger.debug("Row added ({} columns)", row.getColumnData().size());
        break;

      case DONE:
      case DONE_IN_PROC:
      case DONE_PROC:
        DoneToken done = (DoneToken) token;
        currentResultSet.setRowCount(done.getRowCount());  // now long → long

        // Assuming DoneToken.getStatus() returns DoneStatus enum with getValue() method
        int statusValue = done.getStatus().getValue();  // adjust if your enum is different

        logger.info("Result set done: {} rows, status=0x{:02X}",
            done.getRowCount(), statusValue);

        // Bit 4 (0x10) = more result sets coming
        if ((statusValue & 0x10) != 0) {
          hasMoreResultSets = true;
          startNewResultSet();
        }
        break;

      case INFO:
        InfoToken info = (InfoToken) token;
        logger.info("Server message [{}] (state {}): {}",
            info.getNumber(), info.getState(), info.getMessage());

        if (info.isError()) {
          hasError = true;
        }
        break;
      case ERROR:
        ErrorToken error = (ErrorToken) token;
        logger.info("Server message [{}] (state {}): {}",
            error.getNumber(), error.getState(), error.getMessage());

        if (error.isError()) {
          hasError = true;
        }
        break;

      case ENV_CHANGE:
        envChangeVisitor.applyEnvChange((EnvChangeToken) token);
        break;

      default:
        logger.debug("Unhandled token type: 0x{:02X}", token.getType());
    }
  }

  // ------------------- Public API for caller -------------------

  /**
   * Returns a defensive copy of the collected result sets.
   *
   * @return list of collected {@link ResultSet} objects
   */
  public List<ResultSet> getResultSets() {
    return new ArrayList<>(resultSets);
  }

  /**
   * Return the first result set, or {@code null} if none were produced.
   *
   * @return first {@link ResultSet} or {@code null}
   */
  public ResultSet getFirstResultSet() {
    return resultSets.isEmpty() ? null : resultSets.get(0);
  }

  /**
   * Indicates whether any server error/info tokens flagged an error during processing.
   *
   * @return {@code true} if an error was observed
   */
  public boolean hasError() {
    return hasError;
  }

  /**
   * Returns {@code true} if multiple result sets were observed or more are expected.
   *
   * @return {@code true} when multiple result sets are present or more are incoming
   */
  public boolean hasMultipleResultSets() {
    return resultSets.size() > 1 || hasMoreResultSets;
  }

  public void clear() {
    resultSets.clear();
    currentMetadata = null;
    currentResultSet = null;
    hasMoreResultSets = false;
    hasError = false;
    startNewResultSet(); // prepare for reuse
  }

  /**
   * Return a synthesized {@link QueryResponse} containing the collected
   * result sets and error flag for callers.
   *
   * @return QueryResponse summarizing the parsed tokens
   */
  QueryResponse getQueryResponse() {
    return new QueryResponse(getResultSets(), hasError());
  }
}
