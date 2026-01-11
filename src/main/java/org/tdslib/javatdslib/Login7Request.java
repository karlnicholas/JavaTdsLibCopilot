package org.tdslib.javatdslib;

import org.tdslib.javatdslib.messages.Message;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Build a TDS Login7 request message with common login fields.
 * Provides fluent setters for user, password, database and application name.
 */
public class Login7Request {
  private String hostName = "";
  private String serverName = "";
  private String userName = "";
  private String password = "";
  private String appName = "JavaTdsClient";
  private String database = "";
  private int packetSize = 4096;
  private int tdsVersion = 0x00000074; // TDS 7.4

  /**
   * Set the user name for the login request.
   *
   * @param userName user name
   * @return this request for chaining
   */
  public Login7Request withUserName(String userName) {
    this.userName = userName;
    return this;
  }

  /**
   * Set the password for the login request.
   *
   * @param password password
   * @return this request for chaining
   */
  public Login7Request withPassword(String password) {
    this.password = password;
    return this;
  }

  /**
   * Set the target database for the login request.
   *
   * @param database database name
   * @return this request for chaining
   */
  public Login7Request withDatabase(String database) {
    this.database = database;
    return this;
  }

  /**
   * Set the application name for the login request.
   *
   * @param appName application name
   * @return this request for chaining
   */
  public Login7Request withAppName(String appName) {
    this.appName = appName;
    return this;
  }

  /**
   * Build and return a TDS Login7 Message containing this request's data.
   *
   * @return Message ready to be sent over the transport
   */
  public Message toMessage() {
    // Calculate total size (fixed part + variable strings)
    byte[] hostBytes = hostName.getBytes(StandardCharsets.UTF_16LE);
    byte[] serverBytes = serverName.getBytes(StandardCharsets.UTF_16LE);
    byte[] userBytes = userName.getBytes(StandardCharsets.UTF_16LE);
    byte[] passBytes = password.getBytes(StandardCharsets.UTF_16LE);
    byte[] appBytes = appName.getBytes(StandardCharsets.UTF_16LE);
    byte[] dbBytes = database.getBytes(StandardCharsets.UTF_16LE);

    int fixed = 94; // up to ClientProgVer
    int variable = hostBytes.length
        + serverBytes.length
        + userBytes.length
        + passBytes.length
        + appBytes.length
        + dbBytes.length;

    int totalLength = fixed + variable;

    ByteBuffer payload = ByteBuffer.allocate(totalLength).order(ByteOrder.LITTLE_ENDIAN);

    // Fixed part
    payload.putInt(totalLength);
    payload.putInt(tdsVersion);
    payload.putInt(packetSize);
    payload.putInt(0); // ClientProgVer
    payload.putInt(0); // PID
    payload.putInt(0); // ConnectionID
    payload.put((byte) 0xE0); // OptionFlags1 - default values (little endian, ASCII, IEEE, etc.)
    payload.put((byte) 0x00); // OptionFlags2
    payload.put((byte) 0x00); // TypeFlags
    payload.put((byte) 0x00); // Collation (later)
    payload.putInt(0); // TimeZone
    payload.putInt(0); // LCID

    // Offsets & lengths (all in characters, not bytes)
    int pos = 94;
    payload.putShort((short) (pos / 2));
    payload.putShort((short) (hostBytes.length / 2));
    pos += hostBytes.length;
    payload.putShort((short) (pos / 2));
    payload.putShort((short) (serverBytes.length / 2));
    pos += serverBytes.length;
    payload.putShort((short) (pos / 2));
    payload.putShort((short) (userBytes.length / 2));
    pos += userBytes.length;
    payload.putShort((short) (pos / 2));
    payload.putShort((short) (passBytes.length / 2));
    pos += passBytes.length;
    payload.putShort((short) (pos / 2));
    payload.putShort((short) (appBytes.length / 2));
    pos += appBytes.length;
    payload.putShort((short) (pos / 2));
    payload.putShort((short) (dbBytes.length / 2));

    // Data section
    payload.put(hostBytes);
    payload.put(serverBytes);
    payload.put(userBytes);
    payload.put(passBytes);
    payload.put(appBytes);
    payload.put(dbBytes);

    payload.flip();

    return new Message(
        (byte) 0x10,           // Login7
        (byte) 0x01,           // EOM
        payload.capacity() + 8,
        (short) 0,
        (short) 1,
        payload,
        System.nanoTime(),
        null
    );
  }
}
