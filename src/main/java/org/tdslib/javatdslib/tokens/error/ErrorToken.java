package org.tdslib.javatdslib.tokens.error;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Represents an ERROR token (0xAA) sent by the server.
 * Contains SQL error details including number, severity, message, etc.
 */
public class ErrorToken extends Token {

  private final long number;
  private final byte state;
  private final byte severity;
  private final String message;
  private final String serverName;
  private final String procName;
  private final long lineNumber;

  /**
   * Create a new ErrorToken.
   *
   * @param type       raw token byte
   * @param number     error number
   * @param state      state byte
   * @param severity   severity byte
   * @param message    error message (may be null)
   * @param serverName server name (may be null)
   * @param procName   procedure name (may be null)
   * @param lineNumber line number associated with the error
   */
  public ErrorToken(
      byte type,
      long number,
      byte state,
      byte severity,
      String message,
      String serverName,
      String procName,
      long lineNumber) {

    super(TokenType.fromValue(type));
    this.number = number;
    this.state = state;
    this.severity = severity;
    this.message = message != null ? message.trim() : "";
    this.serverName = serverName != null ? serverName.trim() : "";
    this.procName = procName != null ? procName.trim() : "";
    this.lineNumber = lineNumber;
  }

  /**
   * Returns the error number reported by the server.
   */
  public long getNumber() {
    return number;
  }

  /**
   * Returns the state byte for the error.
   */
  public byte getState() {
    return state;
  }

  /**
   * Returns the severity level for this error.
   */
  public byte getSeverity() {
    return severity;
  }

  /**
   * Returns the error message text (trimmed, never null).
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns the server name reported with the error (may be empty).
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * Returns the stored procedure name reported with the error (may be empty).
   */
  public String getProcName() {
    return procName;
  }

  /**
   * Returns the line number associated with the error, if any.
   */
  public long getLineNumber() {
    return lineNumber;
  }

  /**
   * True if this token represents an informational message rather than an error.
   */
  public boolean isInfoMessage() {
    return severity <= 10;
  }

  /**
   * True if this token represents an error (severity &gt; 10).
   */
  public boolean isError() {
    return severity > 10;
  }

  @Override
  public String toString() {
    return String.format(
            "ErrorToken{Msg %d, Level %d, State %d, Line %d, %s: '%s', Server: '%s', Proc: '%s'}",
            number,
            severity & 0xFF,       // Mask to unsigned integer for display
            state & 0xFF,          // Mask to unsigned integer for display
            lineNumber,
            isError() ? "Error" : "Info",
            message,
            serverName,
            procName
    );
  }
}
