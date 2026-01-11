package org.tdslib.javatdslib.tokens.loginack;

import org.tdslib.javatdslib.TdsVersion;
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

    /**
     * Constructs a LoginAckToken.
     *
     * @param type          raw token byte
     * @param interfaceType SQL interface type
     * @param tdsVersion    negotiated TDS version
     * @param serverName    reported server name
     * @param serverVersion reported server product version
     */
    public LoginAckToken(final byte type,
                         final SqlInterfaceType interfaceType,
                         final TdsVersion tdsVersion,
                         final String serverName,
                         final ServerVersion serverVersion) {
        super(TokenType.fromValue(type));
        this.interfaceType = interfaceType;
        this.tdsVersion = tdsVersion;
        this.serverName = serverName;
        this.serverVersion = serverVersion != null ? serverVersion : ServerVersion.UNKNOWN;
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
     * Convenience method: returns the server version as string (e.g. "2022.0").
     */
    public String getServerVersionString() {
        return serverVersion.toVersionString();
    }

    @Override
    public String toString() {
        return String.format(
                "LoginAckToken[InterfaceType=%s, TdsVersion=%s, ServerName=%s, ServerVersion=%s]",
                interfaceType, tdsVersion, serverName, serverVersion
        );
    }
}
