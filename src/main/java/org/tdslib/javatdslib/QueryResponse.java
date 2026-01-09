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

    public QueryResponse(List<ResultSet> resultSets, boolean hasError ) {
        this.resultSets = resultSets;
        this.hasError = hasError;
    }
    public List<ResultSet> getResultSets() {
        return resultSets;
    }
    public boolean hasError() {
        return hasError;
    }
}