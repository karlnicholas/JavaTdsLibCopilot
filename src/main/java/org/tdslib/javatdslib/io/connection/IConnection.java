// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.io.connection;

import java.nio.ByteBuffer;

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
    void startTLS();

    /**
     * Sends data.
     */
    void sendData(ByteBuffer byteBuffer);

    /**
     * Receives data.
     */
    ByteBuffer receiveData();

    /**
     * Clear all incoming data.
     */
    void clearIncomingData();

    void stopTls();
}