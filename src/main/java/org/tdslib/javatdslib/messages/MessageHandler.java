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
import java.util.function.Function;

/**
 * Message handler for processing TDS protocol communication.
 */
public class MessageHandler {
//    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
//    private final TdsClient client;
//
//    // Stateful buffer to hold partial data between network reads
//    private ByteBuffer incomingBuffer = ByteBuffer.allocate(0);
//
//    public MessageHandler(TdsClient client) {
//        this.client = client;
//    }
//
//    /**
//     * Receives a message from the SQL Server using a custom payload factory.
//     */
//    public Message receiveMessage(Function<ByteBuffer, Payload> payloadFunction) {
//        logger.debug("Attempting to receive message...");
//
//        try {
//            RawResult result = receiveMessageRaw();
//
//            PacketType type = result.type;
//            ByteBuffer payloadBuffer = result.payload;
//
//            Message message = new Message(type);
//            message.setPayload(payloadFunction.apply(payloadBuffer));
//
//            logger.debug("Message received completely. Type={}, PayloadSize={}", type, payloadBuffer.remaining());
//            return message;
//
//        } catch (Exception ex) {
//            // Reset buffer on error to prevent corrupted state
//            incomingBuffer = ByteBuffer.allocate(0);
//            logger.error("Error receiving message from SQL Server", ex);
//            throw new RuntimeException(ex);
//        }
//    }
//
//    /**
//     * Receives a message from the SQL Server using the default RawPayload.
//     */
//    public Message receiveMessage() {
//        return receiveMessage(RawPayload::new);
//    }
//
//    /**
//     * Internal result container.
//     */
//    private static class RawResult {
//        final PacketType type;
//        final ByteBuffer payload;
//
//        RawResult(PacketType type, ByteBuffer payload) {
//            this.type = type;
//            this.payload = payload;
//        }
//    }
//
//    /**
//     * Loops to accumulate packets until the last packet is received.
//     */
//    private RawResult receiveMessageRaw() {
//        List<Packet> packetList = new ArrayList<>();
//        boolean isLast = false;
//
//        while (!isLast) {
//            // 1. Wait for Header (8 bytes) to determine packet length
//            waitForData(Packet.HEADER_LENGTH);
//
//            // 2. Read Length from Header (Bytes 2-3, Big Endian)
//            // Peek without moving position to get the full packet size
//            int packetLength = Short.toUnsignedInt(incomingBuffer.getShort(incomingBuffer.position() + 2));
//
//            // 3. Wait for the full packet data (Header + Body)
//            waitForData(packetLength);
//
//            // 4. Extract Packet
//            // Create a slice for just this packet
//            ByteBuffer packetBuffer = incomingBuffer.slice();
//            packetBuffer.limit(packetLength);
//            packetBuffer.order(ByteOrder.BIG_ENDIAN); // TDS Headers are BE
//
//            Packet packet = new Packet(packetBuffer);
//            packetList.add(packet);
//
//            logger.trace("Packet received: Type={}, Length={}, Last={}", packet.getType(), packet.getLength(), packet.isLast());
//
//            // 5. Update incomingBuffer: Move past the consumed packet
//            int pPos = incomingBuffer.position();
//            incomingBuffer.position(pPos + packetLength);
//
//            // Compact or slice the tail to retain remaining bytes
//            if (incomingBuffer.hasRemaining()) {
//                ByteBuffer tail = incomingBuffer.slice();
//                // Copy to new buffer to release reference to the old large buffer
//                ByteBuffer newBuf = ByteBuffer.allocate(tail.remaining());
//                newBuf.put(tail);
//                newBuf.flip();
//                incomingBuffer = newBuf;
//            } else {
//                incomingBuffer = ByteBuffer.allocate(0);
//            }
//
//            // 6. Check if this was the last packet
//            isLast = packet.isLast();
//        }
//
//        if (packetList.isEmpty()) {
//            return new RawResult(PacketType.UNKNOWN, ByteBuffer.allocate(0));
//        }
//
//        // 7. Aggregate Results
//        PacketType type = packetList.get(0).getType();
//
//        // Calculate total payload size
//        int totalSize = packetList.stream().mapToInt(p -> p.getData().remaining()).sum();
//        ByteBuffer fullPayload = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
//
//        for (Packet p : packetList) {
//            fullPayload.put(p.getData());
//        }
//        fullPayload.flip(); // Prepare for reading
//
//        return new RawResult(type, fullPayload);
//    }
//
//    /**
//     * Blocks until incomingBuffer contains at least neededSize bytes.
//     * Fetches more data from the connection if necessary.
//     */
//    private void waitForData(int neededSize) {
//        while (incomingBuffer.remaining() < neededSize) {
//            // This call blocks until data arrives
//            ByteBuffer newChunk = client.getConnection().receiveData();
//
//            // Merge old buffer with new chunk
//            int total = incomingBuffer.remaining() + newChunk.remaining();
//            ByteBuffer grown = ByteBuffer.allocate(total);
//
//            grown.put(incomingBuffer);
//            grown.put(newChunk);
//            grown.flip(); // Prepare for reading
//
//            incomingBuffer = grown;
//        }
//    }
//
//    /**
//     * Sends a message to the SQL Server by fragmenting it into packets.
//     */
//    public void sendMessage(Message message) {
//        int packetSize = org.tdslib.javatdslib.TdsConstants.DEFAULT_PACKET_SIZE;
//        var packets = message.getPackets(packetSize);
//
//        logger.debug("Sending message: type={}, packetCount={}", message.getPacketType(), packets.size());
//
//        try {
//            for (int i = 0; i < packets.size(); i++) {
//                Packet packet = packets.get(i);
//                logger.trace("Sending packet {}/{} (Size: {})", i + 1, packets.size(), packet.getLength());
//
//                // Blocks until written
//                client.getConnection().sendData(packet.getBuffer());
//            }
//            logger.debug("Message sent successfully.");
//        } catch (Exception ex) {
//            logger.error("Failed to send TDS message.", ex);
//            throw new RuntimeException("Failed to send TDS message", ex);
//        }
//    }
}