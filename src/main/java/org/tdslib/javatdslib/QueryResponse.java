package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Stateful visitor that collects the results of one or more result-sets
 * from a SQL batch execution.
 */
public class QueryResponse {
  private static final Logger logger = LoggerFactory.getLogger(QueryResponse.class);

  // ------------------- State -------------------
  private final List<ResultSet> resultSets;  // one per result set
  private final boolean hasError;
  // ------------------------------------------------

  /**
   * Create a QueryResponse containing the collected result sets and an error flag.
   *
   * @param resultSets the list of collected {@link ResultSet} objects
   * @param hasError true if an error was observed while processing the query
   */
  public QueryResponse(List<ResultSet> resultSets, boolean hasError) {
    this.resultSets = resultSets;
    this.hasError = hasError;
  }

  /**
   * Returns the collected result sets.
   *
   * @return a list of {@link ResultSet} objects (may be empty)
   */
  public List<ResultSet> getResultSets() {
    return resultSets;
  }

  /**
   * Indicates whether an error was observed while processing the query.
   *
   * @return {@code true} if an error occurred, otherwise {@code false}
   */
  public boolean hasError() {
    return hasError;
  }
}
