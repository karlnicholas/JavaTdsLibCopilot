// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.messages;

import com.microsoft.data.tools.tdslib.TdsClient;
import com.microsoft.data.tools.tdslib.packets.Packet;
import com.microsoft.data.tools.tdslib.payloads.RawPayload;

import java.util.concurrent.CompletableFuture;

/**
 * Message handler.
 */
public class MessageHandler {
    private final TdsClient client;

    public MessageHandler(TdsClient client) {
        this.client = client;
    }

    /**
     * Receives a message from the SQL Server.
     */
    public CompletableFuture<Message> receiveMessage() {
        return client.getConnection().receiveData()
            .thenApply(buffer -> {
                Packet packet = new Packet(buffer);
                Message message = new Message(packet.getType());
                message.setResetConnection(packet.isResetConnection());
                message.setIgnore(packet.isIgnore());
                message.setPayload(new RawPayload(packet.getData()));
                return message;
            });
    }
}