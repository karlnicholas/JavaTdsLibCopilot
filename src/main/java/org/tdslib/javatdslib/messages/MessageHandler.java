// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.messages;

import org.tdslib.javatdslib.TdsClient;
import org.tdslib.javatdslib.packets.Packet;
import org.tdslib.javatdslib.payloads.RawPayload;

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