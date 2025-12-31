// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.TdsClient;
import org.tdslib.javatdslib.packets.Packet;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.payloads.Payload;
import org.tdslib.javatdslib.payloads.RawPayload;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * Message handler for processing TDS protocol communication.
 */
public class MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
    private final TdsClient client;

    // Stateful buffer to hold partial data between network reads (Simulates C# incomingMessageBuffer)
    private ByteBuffer incomingBuffer = ByteBuffer.allocate(0);

    public MessageHandler(TdsClient client) {
        this.client = client;
    }

    /**
     * Receives a message from the SQL Server.
     * Corresponds to C#: public async Task<Message> ReceiveMessage(...)
     */
    public CompletableFuture<Message> receiveMessage(Function<ByteBuffer, Payload> payloadFunction) {
        logger.debug("Attempting to receive message...");

        // Start the recursive packet accumulation loop
        return receiveMessageRaw()
                .thenApply(result -> {
                    PacketType type = result.type;
                    ByteBuffer payloadBuffer = result.payload;

                    Message message = new Message(type);
                    // Flags are technically per-packet, but usually consistent for the message.
                    // We take them from the last packet context or defaults.
                    // (Simplification: Assuming standard message behavior)
//                    message.setPayload(new RawPayload(payloadBuffer));
                    message.setPayload(payloadFunction.apply(payloadBuffer));

                    logger.debug("Message received completely. Type={}, PayloadSize={}", type, payloadBuffer.remaining());
                    return message;
                })
                .exceptionally(ex -> {
                    // Reset buffer on error (simulating C# catch block logic)
                    incomingBuffer = ByteBuffer.allocate(0);
                    logger.error("Error receiving message from SQL Server", ex);
                    throw new CompletionException(ex);
                });
    }
    /**
     * Receives a message from the SQL Server.
     * Corresponds to C#: public async Task<Message> ReceiveMessage(...)
     */
    public CompletableFuture<Message> receiveMessage() {
        logger.debug("Attempting to receive message...");

        // Start the recursive packet accumulation loop
        return receiveMessageRaw()
                .thenApply(result -> {
                    PacketType type = result.type;
                    ByteBuffer payloadBuffer = result.payload;

                    Message message = new Message(type);
                    // Flags are technically per-packet, but usually consistent for the message.
                    // We take them from the last packet context or defaults.
                    // (Simplification: Assuming standard message behavior)
                    message.setPayload(new RawPayload(payloadBuffer));

                    logger.debug("Message received completely. Type={}, PayloadSize={}", type, payloadBuffer.remaining());
                    return message;
                })
                .exceptionally(ex -> {
                    // Reset buffer on error (simulating C# catch block logic)
                    incomingBuffer = ByteBuffer.allocate(0);
                    logger.error("Error receiving message from SQL Server", ex);
                    throw new CompletionException(ex);
                });
    }

    /**
     * Internal result container for the raw loop.
     */
    private static class RawResult {
        final PacketType type;
        final ByteBuffer payload;

        RawResult(PacketType type, ByteBuffer payload) {
            this.type = type;
            this.payload = payload;
        }
    }

    /**
     * Corresponds to C#: private async Task<(PacketType, ByteBuffer)> ReceiveMessageRaw(...)
     */
    private CompletableFuture<RawResult> receiveMessageRaw() {
        // List to hold fragments
        List<Packet> packetList = new ArrayList<>();

        // Start the recursive loop
        return processNextPacket(packetList)
                .thenApply(v -> {
                    if (packetList.isEmpty()) {
                        return new RawResult(PacketType.UNKNOWN, ByteBuffer.allocate(0));
                    }

                    // 1. Determine Packet Type (from first packet)
                    PacketType type = packetList.get(0).getType();

                    // 2. Concatenate Payloads
                    // C#: new ByteBuffer(packetList.Select(p => p.Data))
                    int totalSize = packetList.stream().mapToInt(p -> p.getData().remaining()).sum();
                    ByteBuffer fullPayload = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

                    for (Packet p : packetList) {
                        fullPayload.put(p.getData());
                    }
                    fullPayload.flip(); // Prepare for reading

                    return new RawResult(type, fullPayload);
                });
    }

    /**
     * Recursive loop logic.
     * Corresponds to C# while(true) loop inside ReceiveMessageRaw.
     */
    private CompletableFuture<Void> processNextPacket(List<Packet> packetList) {
        // 1. Wait for Header (8 bytes)
        return waitForData(Packet.HEADER_LENGTH)
                .thenCompose(v -> {
                    // 2. Read Length from Header (Bytes 2-3, Big Endian)
                    // Peek without moving position
                    int packetLength = Short.toUnsignedInt(incomingBuffer.getShort(incomingBuffer.position() + 2));

                    // 3. Wait for the full packet data
                    return waitForData(packetLength)
                            .thenApply(v2 -> packetLength); // Pass length down chain
                })
                .thenCompose(packetLength -> {
                    // 4. Extract Packet
                    // C#: Packet packet = new Packet(incomingMessageBuffer.Slice(0, packetLength));

                    // Create view for the packet
                    ByteBuffer packetBuffer = incomingBuffer.slice();
                    packetBuffer.limit(packetLength);
                    packetBuffer.order(ByteOrder.BIG_ENDIAN); // Headers are BE

                    Packet packet = new Packet(packetBuffer);
                    packetList.add(packet);

                    logger.trace("Packet received: Type={}, Length={}, Last={}", packet.getType(), packet.getLength(), packet.isLast());

                    // 5. Slice off the consumed packet from incomingBuffer
                    // C#: incomingMessageBuffer = incomingMessageBuffer.Slice(packetLength);
                    int pPos = incomingBuffer.position();
                    incomingBuffer.position(pPos + packetLength);

                    // Compact or slice the tail
                    if (incomingBuffer.hasRemaining()) {
                        ByteBuffer tail = incomingBuffer.slice();
                        // Copy to new buffer to release reference to old large buffer
                        ByteBuffer newBuf = ByteBuffer.allocate(tail.remaining());
                        newBuf.put(tail);
                        newBuf.flip();
                        incomingBuffer = newBuf;
                    } else {
                        incomingBuffer = ByteBuffer.allocate(0);
                    }

                    // 6. Check for Last packet or Recurse
                    if (packet.isLast()) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return processNextPacket(packetList); // Loop
                    }
                });
    }

    /**
     * Corresponds to C#: private async Task WaitForData(int size, ...)
     */
    private CompletableFuture<Void> waitForData(int neededSize) {
        if (incomingBuffer.remaining() >= neededSize) {
            return CompletableFuture.completedFuture(null);
        }

        // Need more data
        return client.getConnection().receiveData()
                .thenCompose(newChunk -> {
                    // C#: incomingMessageBuffer = incomingMessageBuffer.Concat(...)
                    // Java: Allocate new, put old, put new
                    int total = incomingBuffer.remaining() + newChunk.remaining();
                    ByteBuffer grown = ByteBuffer.allocate(total);

                    grown.put(incomingBuffer);
                    grown.put(newChunk);
                    grown.flip(); // Prepare for reading

                    incomingBuffer = grown;

                    // Recurse to check if we have enough now
                    return waitForData(neededSize);
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
                // Use getBuffer() to send the full Header + Data
                return client.getConnection().sendData(packet.getBuffer());
            });
        }

        return pipeline.handle((result, ex) -> {
            if (ex != null) {
                logger.error("Failed to send TDS message.", ex);
                throw new CompletionException(ex);
            }
            logger.debug("Message sent successfully.");
            return result;
        });
    }
}