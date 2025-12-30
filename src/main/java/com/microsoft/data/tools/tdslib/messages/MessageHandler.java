// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.messages;

import com.microsoft.data.tools.tdslib.TdsClient;
import com.microsoft.data.tools.tdslib.packets.PacketType;
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
                Message message = new Message(PacketType.TABULAR_RESULT); // TODO: Determine packet type
                message.setPayload(new RawPayload(buffer));
                return message;
            });
    }
}