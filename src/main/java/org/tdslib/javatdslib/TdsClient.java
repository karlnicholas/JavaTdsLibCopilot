// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib;

import org.tdslib.javatdslib.io.connection.IConnection;
import org.tdslib.javatdslib.io.connection.tcp.TcpConnection;
import org.tdslib.javatdslib.io.connection.tcp.TcpConnectionOptions;
import org.tdslib.javatdslib.io.connection.tcp.TcpServerEndpoint;
import org.tdslib.javatdslib.messages.MessageHandler;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * TDS Client.
 */
public class TdsClient implements AutoCloseable {
    private IConnection connection;
    private MessageHandler messageHandler;
    private TokenStreamHandler tokenStreamHandler;

    /**
     * Underlying connection used to communicate with the SQL Server.
     */
    public IConnection getConnection() {
        return connection;
    }

    /**
     * The MessageHandler of this client.
     */
    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

    /**
     * The TokenStreamHandler of this client.
     */
    public TokenStreamHandler getTokenStreamHandler() {
        return tokenStreamHandler;
    }

    /**
     * Creates a new TDS Client and establishes a Tcp connection to the endpoint specified by the TcpServerEndpoint using default connection options.
     */
    public TdsClient(TcpServerEndpoint serverEndpoint) throws IOException {
        this(new TcpConnectionOptions(), serverEndpoint);
    }

    /**
     * Creates a new TDS Client and establishes a Tcp connection to the endpoint specified by the TcpServerEndpoint using the specified ConnectionOptions.
     */
    public TdsClient(TcpConnectionOptions options, TcpServerEndpoint serverEndpoint) throws IOException {
        this(new TcpConnection(options, serverEndpoint));
    }

    /**
     * Creates a new TDS Client with a connection to a SQL Server.
     */
    public TdsClient(IConnection connection) {
        this.connection = connection != null ? connection : null; // throw new NullPointerException
        this.messageHandler = new MessageHandler(this);
        this.tokenStreamHandler = new TokenStreamHandler(this);
    }

    /**
     * Performs the TLS handshake between the client and the database server.
     */
    public CompletableFuture<Void> performTlsHandshake() {
        return connection.startTLS();
    }

    /**
     * Closes the connection to the actual database server and re-establishes a Tcp connection to a new database server endpoint.
     */
    public void reEstablishConnection(TcpConnectionOptions options, TcpServerEndpoint serverEndpoint) throws Exception {
        connection.close();
        connection = new TcpConnection(options, serverEndpoint);
    }

    /**
     * Closes the connection to the actual database server and re-establishes a connection to a SQL Server.
     */
    public void reEstablishConnection(IConnection connection) throws Exception {
        this.connection.close();
        this.connection = connection != null ? connection : null;
    }

    /**
     * Disposes resources from this TDS client and underlying components.
     */
    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}