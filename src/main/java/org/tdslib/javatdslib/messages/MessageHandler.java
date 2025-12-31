// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.TdsClient;
import org.tdslib.javatdslib.packets.Packet;
import org.tdslib.javatdslib.payloads.RawPayload;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Message handler for processing TDS protocol communication.
 */
public class MessageHandler {
    // Standard SLF4J Logger
    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private final TdsClient client;

    public MessageHandler(TdsClient client) {
        this.client = client;
    }

    /**
     * Receives a message from the SQL Server.
     */
    public CompletableFuture<Message> receiveMessage() {
        logger.debug("Attempting to receive data from connection...");

        return client.getConnection().receiveData()
                .thenApply(buffer -> {
//                    Packet packet = new Packet(buffer);
                    Packet packet = new Packet(buffer.slice().order(java.nio.ByteOrder.BIG_ENDIAN));
                    logger.trace("Packet received: type={}, size={}", packet.getType(), buffer.remaining());

                    Message message = new Message(packet.getType());
                    message.setResetConnection(packet.isResetConnection());
                    message.setIgnore(packet.isIgnore());
                    message.setPayload(new RawPayload(packet.getData()));

                    return message;
                })
                .exceptionally(ex -> {
                    logger.error("Error receiving message from SQL Server", ex);
                    throw new CompletionException(ex);
                });
    }

    /**
     * Sends a message to the SQL Server by fragmenting it into packets.
     */
    public CompletableFuture<Void> sendMessage(Message message) {
        int packetSize = org.tdslib.javatdslib.TdsConstants.DEFAULT_PACKET_SIZE;
        var packets = message.getPackets(packetSize);

        logger.debug("Sending message: type={}, packetCount={}", message.getPacketType(), packets.size());

        CompletableFuture<Void> pipeline = CompletableFuture.completedFuture(null);

        for (int i = 0; i < packets.size(); i++) {
            final var packet = packets.get(i);
            final int index = i + 1;

            pipeline = pipeline.thenCompose(v -> {
                logger.trace("Sending packet {}/{} (Size: {})", index, packets.size(), packet.getLength());

                // FIX: Send getBuffer() (Header + Data), NOT getData() (Data only)
                return client.getConnection().sendData(packet.getBuffer());
            });
        }

        return pipeline.handle((result, ex) -> {
            if (ex != null) {
                logger.error("Failed to send TDS message. Packets sent: {}/{}", packets.size(), packets.size(), ex);
                throw new CompletionException(ex);
            }
            logger.debug("Message sent successfully.");
            return result;
        });
    }
}