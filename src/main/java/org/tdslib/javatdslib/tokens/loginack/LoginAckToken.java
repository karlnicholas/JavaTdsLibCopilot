package org.tdslib.javatdslib.tokens.loginack;

import org.tdslib.javatdslib.TdsVersion; // The TDS protocol version enum
import org.tdslib.javatdslib.tokens.loginack.ServerVersion; // The new SQL Server product version enum
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * LOGINACK token (0xAD) - sent by server after successful login.
 */
public class LoginAckToken extends Token {

    private final SqlInterfaceType interfaceType;
    private final TdsVersion tdsVersion;
    private final String serverName;
    private final ServerVersion serverVersion;

    public LoginAckToken(SqlInterfaceType interfaceType, TdsVersion tdsVersion, String serverName, ServerVersion serverVersion) {
        this.interfaceType = interfaceType;
        this.tdsVersion = tdsVersion;
        this.serverName = serverName;
        this.serverVersion = serverVersion != null ? serverVersion : ServerVersion.UNKNOWN;
    }

    @Override
    public TokenType getType() {
        return TokenType.LOGIN_ACK;
    }

    public SqlInterfaceType getInterfaceType() {
        return interfaceType;
    }

    public TdsVersion getTdsVersion() {
        return tdsVersion;
    }

    public String getServerName() {
        return serverName;
    }

    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    /**
     * Convenience method: returns the server version as string (e.g. "SQL Server 2022").
     */
    public String getServerVersionString() {
        return serverVersion.toVersionString();
    }

    @Override
    public String toString() {
        return "LoginAckToken[" +
                "InterfaceType=" + interfaceType +
                ", TdsVersion=" + tdsVersion +
                ", ServerName=" + serverName +
                ", ServerVersion=" + serverVersion +
                ']';
    }
}