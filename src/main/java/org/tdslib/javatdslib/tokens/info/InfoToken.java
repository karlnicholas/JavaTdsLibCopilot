package org.tdslib.javatdslib.tokens.info;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * INFO token (0xAB) - informational message from the server (severity <= 10).
 */
public final class InfoToken extends Token {

  private final long number;
  private final byte state;
  private final byte severity;
  private final String message;
  private final String serverName;
  private final String procName;
  private final long lineNumber;

  /**
   * Constructs a new InfoToken instance.
   *
   * @param type       raw token type byte
   * @param number     message number
   * @param state      state byte
   * @param severity   severity byte
   * @param message    message text (may be null)
   * @param serverName server name (may be null)
   * @param procName   procedure name (may be null)
   * @param lineNumber associated line number
   */
  public InfoToken(
      final byte type,
      final long number,
      final byte state,
      final byte severity,
      final String message,
      final String serverName,
      final String procName,
      final long lineNumber) {

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
   * Returns the message number.
   */
  public long getNumber() {
    return number;
  }

  /**
   * Returns the state byte.
   */
  public byte getState() {
    return state;
  }

  /**
   * Returns the severity byte.
   */
  public byte getSeverity() {
    return severity;
  }

  /**
   * Returns the message text (never null).
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns the server name (may be empty).
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * Returns the stored procedure name (may be empty).
   */
  public String getProcName() {
    return procName;
  }

  /**
   * Returns the line number associated with the message.
   */
  public long getLineNumber() {
    return lineNumber;
  }

  /**
   * Returns true if this is an error message (severity &gt; 10 in TDS).
   */
  public boolean isError() {
    return severity > 10;
  }

  @Override
  public String toString() {
    final String fmt = "InfoToken{number=%d, severity=%d, state=%d, "
        + "message='%s', server='%s', proc='%s', line=%d}";
    return String.format(
        fmt,
        number,
        severity,
        state,
        message,
        serverName,
        procName,
        lineNumber
    );
  }
}
