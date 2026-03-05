package org.tdslib.javatdslib.protocol;

/**
 * A generic, internal exception representing an ERROR token received from SQL Server. Decoupled from
 * any specific database API (like JDBC or R2DBC).
 */
public class TdsServerErrorException extends RuntimeException {
  private final int errorNumber;
  private final int state;
  private final int severity;
  private final String serverName;
  private final String procName;
  private final int lineNumber;

  /**
   * Constructs a new TdsServerErrorException.
   *
   * @param message The error message.
   * @param errorNumber The error number.
   * @param state The error state.
   * @param severity The error severity.
   * @param serverName The name of the server where the error occurred.
   * @param procName The name of the stored procedure where the error occurred.
   * @param lineNumber The line number where the error occurred.
   */
  public TdsServerErrorException(
      String message,
      int errorNumber,
      int state,
      int severity,
      String serverName,
      String procName,
      int lineNumber) {
    super(message);
    this.errorNumber = errorNumber;
    this.state = state;
    this.severity = severity;
    this.serverName = serverName;
    this.procName = procName;
    this.lineNumber = lineNumber;
  }

  public int getErrorNumber() {
    return errorNumber;
  }

  public int getState() {
    return state;
  }

  public int getSeverity() {
    return severity;
  }

  public String getServerName() {
    return serverName;
  }

  public String getProcName() {
    return procName;
  }

  public int getLineNumber() {
    return lineNumber;
  }
}
