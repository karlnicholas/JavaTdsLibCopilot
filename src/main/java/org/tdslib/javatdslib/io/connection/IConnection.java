// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.io.connection;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Connection to the SQL Server.
 */
public interface IConnection extends AutoCloseable {

    /**
     * Connection options.
     */
    ConnectionOptions getOptions();

    /**
     * Starts the SSL/TLS by performing a handshake with the SQL Server.
     */
    CompletableFuture<Void> startTLS();

    /**
     * Sends data.
     */
    CompletableFuture<Void> sendData(ByteBuffer byteBuffer);

    /**
     * Receives data.
     */
    CompletableFuture<ByteBuffer> receiveData();

    /**
     * Clear all incoming data.
     */
    void clearIncomingData();

}