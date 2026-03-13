# Architecture: Reactive TDS Stream Processing

This document details the internal architecture of the reactive Tabular Data Stream (TDS) processing engine. The driver is built to handle fully unbuffered, asynchronous network streams, utilizing a state-machine approach to prevent thread blocking, race conditions, and network starvation.

## 1. The Client Entry Point: `TdsResult` and Thread Decoupling

When a client executes a query, the materialized results are exposed via the `TdsResult` class. To prevent user-defined mapping logic from blocking the critical network I/O threads, the architecture strictly enforces a thread boundary at this layer.

### The Mapper Worker Thread

When `Result.map()` is invoked, it instantiates a dedicated `ExecutorService` (e.g., `Tds-Mapper-Worker`).

* Every time a `Result.RowSegment` arrives from the upstream publisher, it is dispatched to this worker thread.
* The user's lambda (`mappingFunction.apply(row, row.getMetadata())`) executes entirely on this thread, shielding the NIO network layer from application-level latency.

### Path 1: Synchronous Auto-Draining

Because TDS is a continuous, unbuffered stream, a row cannot be bypassed on the wire; every byte must be consumed or explicitly discarded.

* Inside the `Tds-Mapper-Worker` thread, a `finally` block guarantees that `((StatefulRow) row).drain()` is called after the user's mapping function completes.
* This "Path 1" hook ensures that if a user maps only a subset of columns (e.g., column 0 but ignores columns 1-5), the driver automatically fast-forwards the cursor, clears the unread bytes from the wire, and safely triggers the network to fetch the next row.
* The next token is requested from upstream *only* after the mapping and subsequent drain are completely successful.

---

## 2. The Row State Machine: `StatefulRow`

The `StatefulRow` acts as the bridge between the raw TDS payload and the user's column requests. It maintains a strict, forward-only cursor (`cursorIndex`) through the row's metadata.

### Thread Visibility and Memory Barriers

Because the `TDS-EventLoop` thread parses the network bytes and the `Tds-Mapper-Worker` thread reads them via the `.get()` method, the row's state must be completely thread-safe.

* The `cursorIndex` and `lobActive` flags are marked as `volatile`.
* This creates a strict memory barrier. When the Event Loop thread finishes processing a Large Object (LOB) and sets `lobActive = false`, the Mapper thread is guaranteed to see the updated state on its next `.get()` invocation, preventing `IllegalStateException` or misaligned reads.

### Synchronous vs. Asynchronous Decoding

When `row.get(index)` is called, the driver advances the `cursorIndex` up to the requested column.

* **Standard Primitives (e.g., INT, BIT):** Decoded synchronously in memory and cached in `rowCache[cursorIndex]`.
* **Large Objects (BLOB/CLOB):** If the data type is a PLP (Partially Length-Prefixed) stream, the row wraps the data in a `TdsBlob` or `TdsClob` proxy hook and immediately sets `lobActive = true`.
* Attempting to advance the row cursor while `lobActive` is true results in an exception to prevent stream corruption.

### The Escape Hatch

When the `cursorIndex` reaches the total number of columns and no LOB is active, `triggerEscapeHatchIfComplete()` is invoked.

* It slices any remaining trailing bytes from the row payload, advances the buffer position to the limit, and hands the bytes back to the `StatefulTokenDecoder`.
* It then commands the `TdsTransport` to `resumeNetworkRead()`, waking up the NIO selector to parse the next logical token (e.g., the `DONE` token).

---

## 3. Dynamic Stream Routing: `PlpStreamHandler`

When a LOB column is accessed, the standard protocol parser (`StatefulTokenDecoder`) is temporarily bypassed. The architecture utilizes real-time dynamic routing to funnel raw network bytes directly to the active LOB consumer.

### Handoff and Memory Ownership

The `PlpStreamHandler` assumes total control of the `ByteBuffer` chunk. To prevent "Phantom Byte Double-Feeds" (where unconsumed bytes are recycled in the next Event Loop cycle), explicit pointer ownership is enforced.

* When the PLP Terminator (`pendingChunkBytes == 0`) is reached, the handler slices the remaining bytes for the next column.
* **Critical Operation:** The handler explicitly advances the parent buffer's position to its limit (`payload.position(payload.limit())`). This signals to the `NioSocketConnection` that the handler has completely claimed the packet, ensuring the buffer is cleanly compacted.

### Reentrant State Cleanup

To prevent race conditions between the Event Loop and Mapper threads, internal state resolution is strictly reordered to execute *before* downstream notification.

* The `onCompleteCallback.accept(remainingBytes)` is invoked first.
* This synchronous callback reaches back into `StatefulRow`, hot-swaps the `TdsStreamHandler` back to the protocol decoder, and sets the `volatile` `lobActive` flag to `false`.
* Only after the internal architecture is completely reset does the handler invoke `subscriber.onComplete()`, which safely wakes up the user's continuation logic.

---

## 4. Starvation-Driven Backpressure: `TdsClob` and `TdsBlob`

To maximize throughput and minimize network chatter, LOB streaming employs a "Wake If Starved" mechanical backpressure model.

* When a `TdsClob` or `TdsBlob` stream is subscribed to, the router target is locked in via `transport.switchStreamHandler(plpHandler)`.
* The handler attempts to synchronously process any bytes left over in memory from the previous column *before* touching the network layer.
* A `boolean[] completed` array tracks if the PLP terminator was reached using only the initial memory chunk.
* **Mechanized Backpressure:** `transport.resumeNetworkRead()` is invoked *only* if `!completed[0]` is true (meaning the LOB is starved and physically requires the next network packet to continue).

---

## 5. Network Control: The NIO Layer

The physical network lifecycle is governed entirely by protocol-driven demand, orchestrated by the `NioSocketConnection` and `TdsChunkDecoder`.

### The Event Loop

* Asynchronous I/O runs on a dedicated daemon thread named `TDS-EventLoop-[port]`.
* Network reads are physically paused by removing `SelectionKey.OP_READ` from the NIO selector via `suspendRead()`.
* **Example:** The instant the `StatefulTokenDecoder` parses a complete `ROW` token, it invokes `transport.suspendNetworkRead()`. This hard-locks the socket, preventing the Event Loop from overrunning the buffer until the application layer is ready to drain the row.

### Payload Extraction

* When the network is active, bytes flow into the `TdsChunkDecoder`.
* The decoder strips the 8-byte TDS header, calculates the exact payload size, and creates a protected `slice()` of the payload.
* This slice guarantees that downstream parsers (whether standard primitives or PLP streams) cannot accidentally read past physical chunk boundaries into the next TDS header.

-----------------

## Architectural Stack: TDS Reactive Library

Based on the investigation of the code and trace logs, the library follows a **Layered Protocol Pattern**. Each layer is responsible for a specific stage of transforming raw network bytes into meaningful database objects.

---

### 1. The Transport Layer: `NioSocketConnection`

This is the lowest level of the library’s stack. Its primary responsibility is the raw movement of bytes between the operating system's network stack and the Java application.

* **Role**: Manages the `SocketChannel`, handles the NIO `Selector` events, and performs the actual `channel.read()` calls.
* **Function**: It receives data from the OS into a local `readBuffer` and notifies the upper layers via the `onDataAvailable` callback.
* **Ownership**: It owns the hardware-level connection and handles low-level backpressure by toggling `OP_READ`.

### 2. The Framing (Chunk) Layer: `TdsChunkDecoder`

This layer acts as the "Envelope Opener." In the TDS protocol, every packet sent by SQL Server is wrapped in an 8-byte header that defines the packet type and length.

* **Role**: Strips the TDS-specific "Framing" from the raw stream.
* **Function**: It parses the 8-byte header to determine the `payload size` and whether the **End of Message (EOM)** bit is set.
* **Handoff**: It creates a `slice()` of the raw buffer—representing just the payload—and hands it to the next layer. This ensures the parser doesn't accidentally read into the next TDS packet's header.
* **EOM** (End of Message) Handling
* **Extraction**: The decoder extracts the EOM bit from the second byte of the TDS header (Status Byte).
* **State Management**: It maintains currentChunkIsEom to track if the current payload is the final segment of the TDS message.

Downstream Notification: This status is passed as a parameter to onPayloadAvailable, allowing the Protocol Layer to correctly terminate the reactive stream once all tokens are parsed.

### 3. The Token/Protocol Layer: `StatefulTokenDecoder`

This is the "Brain" of the library. It understands the TDS Grammar. It doesn't know about "Rows" yet; it knows about "Tokens."

* **Role**: Converts the raw payload into a sequence of protocol tokens (e.g., `ColMetaDataToken`, `RowToken`, `DoneToken`).
* **The "Stateful" Nature**: Because tokens can be split across multiple network packets, this layer maintains an `accumulator` buffer to reassemble fragmented data.
* **The Bridge**: This is where the **Mid-Packet Handoff** occurs. It identifies the start of a row and triggers the event that eventually wakes up the `Tds-Mapper-Worker` thread.

### 4. The Reactive/Mapping Layer: `StatefulRow`

This is the highest layer and the one your application code directly interacts with.

* **Role**: Data Materialization. It turns a `RowToken` into a Java-friendly row object.
* **Function**: It maps the raw bytes for each column into specific Java types (like `Integer` or `String`) based on the metadata it received earlier.
* **Backpressure**: It handles the logic for **"Auto-Drain"** if the user code doesn't read every column, ensuring the underlying `StatefulTokenDecoder` stays in sync with the protocol.

---

### Summary Table

| Layer | Class | Responsibility | Analogy |
| --- | --- | --- | --- |
| **Transport** | `NioSocketConnection` | Reading bytes from the OS. | The Mail Truck delivering a box. |
| **Framing** | `TdsChunkDecoder` | Stripping the 8-byte TDS header. | Opening the shipping box to find the contents. |
| **Protocol** | `StatefulTokenDecoder` | Identifying Row/Done/Error tokens. | Reading the table of contents of a book. |
| **Mapping** | `StatefulRow` | Converting bytes to `Integer`, `String`, etc. | Translating the text of the book into your language. |

-----------------------------------------

This documentation analysis outlines the dynamic stream routing architecture of the TDS library, focusing on how it manages the transition between protocol-level token parsing and high-performance LOB (Large Object) data streaming.

---

## Architectural Analysis: Dynamic Stream Routing

The library utilizes a **Hot-Swappable Handler Pattern** to manage the complexity of the Tabular Data Stream (TDS) protocol. This allows the transport layer to shift between "Control Plane" decoding (parsing tokens like Rows and Errors) and "Data Plane" decoding (streaming large binary or text data) without restarting the network connection.

### 1. The Orchestrator: `TdsTransport`

The `TdsTransport` class acts as the central router for all incoming network traffic during the asynchronous query phase.

* **Dynamic Routing**: It maintains a reference to a `currentStreamHandler`. Instead of hardcoding a single path for data, it utilizes a dynamic lambda (the `dynamicRouter`) that redirects every incoming payload slice to whichever handler is currently active.
* **State Switching**: Through the `switchStreamHandler` method, the library can "hot-swap" the receiver of network bytes in real-time. This is essential for handling Partially Length-Prefixed (PLP) data, such as `VARCHAR(MAX)` or `VARBINARY(MAX)`, where the stream transitions from standard tokens to raw data chunks.
* **Backpressure Propagation**: It provides a bridge for handlers to signal backpressure (via `suspendNetworkRead` and `resumeNetworkRead`) directly to the underlying `NioSocketConnection`.

### 2. The Control Plane: `StatefulTokenDecoder`

This is the default handler for standard query responses. It is responsible for the logical structure of the TDS protocol.

* **Token Identification**: It scans the payload for TDS tokens (e.g., `ENVCHANGE`, `COLMETADATA`, `ROW`).
* **Buffer Accumulation**: Because network packets rarely align perfectly with protocol tokens, it maintains an `accumulator` to reassemble split tokens across multiple packets.
* **Re-entrancy Management**: It utilizes an `AtomicBoolean` (`isParsing`) to handle re-entrant calls. If the mapper thread triggers a request for more data while the loop is still active, it saves the bytes to `pendingReentrantBytes` to ensure ordered processing.

### 3. The Data Plane: `PlpStreamHandler`

When the stream encounters a Large Object (LOB), the library swaps the `StatefulTokenDecoder` for the `PlpStreamHandler`.

* **Bypassing the Parser**: By switching to this handler, the driver stops trying to parse bytes as TDS tokens. This significantly improves performance for large data transfers by eliminating token overhead.
* **Chunk Reassembly**: It parses the specialized PLP "chunk-length" headers found in SQL Server large-data streams. It handles the extraction of raw data bytes and forwards them to a reactive `Subscriber`.
* **The Terminator and Reversion**: Once the PLP Terminator (a chunk length of 0) is reached, the handler executes an `onCompleteCallback`. This callback is typically used to swap the `currentStreamHandler` back to the original `StatefulTokenDecoder` so the driver can resume parsing standard tokens (like the final `DONE` token).

---

### Stream Lifecycle Summary

| Phase | Active Handler | Responsibility |
| --- | --- | --- |
| **Metadata** | `StatefulTokenDecoder` | Parsing `COLMETADATA` and standard `ROW` tokens. |
| **LOB Detected** | `PlpStreamHandler` | Bypassing token logic to stream raw `VARBINARY(MAX)` chunks. |
| **LOB Finished** | `StatefulTokenDecoder` | Re-taking control to find the `DONE` token and complete the query. |

### Critical Requirement: Handoff Integrity

Both handlers implement the `TdsStreamHandler` interface, which carries a strict contract: the handler **must** advance the `ByteBuffer` position of the payload it consumes. As seen in the `PlpStreamHandler` cleanup, failing to advance the position to the limit during a handler swap would cause the remaining bytes of a packet to be double-processed or lost when the new handler takes over.

Would you like to analyze the `onCompleteCallback` implementation to see exactly how the `TdsTransport` manages the transition back from the `PlpStreamHandler` to the `StatefulTokenDecoder`?

-----------------------------


Here is a formal review of the Architecture Guidelines for the Reactive TDS Stream Processing Engine. These are the five immutable pillars that govern how this driver must operate to maintain stability, high throughput, and R2DBC compliance without buffering massive datasets in memory.

1. Strict Thread Decoupling
   The architecture enforces a hard boundary between physical network I/O and user-defined application logic.

The I/O Domain (TDS-EventLoop): A dedicated daemon thread strictly responsible for polling the NIO Selector, stripping TDS headers, and identifying logical protocol boundaries (e.g., the start of a ROW token). It must never execute user code.

The Application Domain (Tds-Mapper-Worker): A separate worker thread that executes the user's Result.map lambda.

The Rule: The Event Loop must never block waiting for the user's lambda to finish, and the user's lambda must never inadvertently execute on the Event Loop stack.

2. Protocol-Driven Backpressure
   Because TDS is an unbuffered, continuous stream, backpressure is managed at the TCP/NIO level rather than through reactive queues.

Suspension on Discovery: The instant the Event Loop parses a complete ROW token, it physically disables SelectionKey.OP_READ on the socket. The network is hard-locked.

Resumption on Demand: The network remains locked until the Tds-Mapper-Worker fully completes its column mapping and explicitly signals that it is ready for the next protocol segment.

3. Dynamic Stream Routing for Large Objects
   Standard primitive columns (INT, BIT) are decoded synchronously. However, Large Objects (LOBs) that span multiple physical packets require bypassing the standard protocol parser.

The Hot-Swap: When a LOB is accessed, the driver dynamically swaps the active network receiver to a specialized PlpStreamHandler.

Raw Throughput: This allows raw network bytes to flow directly from the physical socket into the R2DBC Clob or Blob publisher, avoiding the overhead of the token parser entirely.

State Reversion: Once the LOB reaches its terminator (0x00), the router is strictly swapped back to the protocol parser before the client is notified of completion.

4. Starvation-Driven LOB Mechanics
   To maximize throughput and minimize network chatter during multi-packet LOB streaming, the driver employs a "Wake If Starved" model.

Memory First: A LOB stream must always attempt to process whatever bytes are currently lingering in memory from the previous read cycle.

Mechanized Wakeup: The network layer is only instructed to fetch the next physical packet if the LOB stream completely exhausts its available memory without reaching the PLP terminator.

5. Forward-Only State Machines & Auto-Draining
   TDS requires that every byte sent by the server is either consumed or explicitly discarded; it is impossible to "skip" a row on the wire.

The Cursor: StatefulRow maintains a strict, forward-only cursor.

The "Path 1" Auto-Drain: If a user maps Column 0 and ignores Columns 1 through 5, the architecture guarantees a synchronous finally hook that fast-forwards the cursor, reading and discarding the remaining columns off the wire.

The Transition: This guarantees the network buffer is perfectly aligned for the start of the next ROW or DONE token before the network is reawakened.

------------------------


To understand how raw network bytes are gathered into logical rows and emitted to the client, we need to trace the exact lifecycle of a ROW token through your architecture.

This process spans across four distinct layers, transforming a continuous stream of hexadecimal bytes into a reactive Result.RowSegment that the client can map.

Here is the step-by-step journey of a row in your current stable codebase:

Phase 1: Token Discovery (StatefulTokenDecoder)
Everything starts in the TDS-EventLoop thread. The StatefulTokenDecoder continuously scans the incoming ByteBuffer looking for TDS token signatures.

The Trigger: The decoder encounters 0xD1 (the ROW token).

The Slice: Instead of parsing every column immediately, it creates a protected window (ByteBuffer.slice()) around the raw payload of that specific row.

The Handoff: It passes this raw payload downstream to the visitor.onToken(token) method and immediately suspends the network to prevent overrunning the buffer.

Phase 2: Row Assembly (ReactiveResultVisitor)
The ReactiveResultVisitor acts as the assembly line, converting raw protocol tokens into R2DBC-compliant objects.

State Injection: When it receives the RawRowToken, it combines the raw payload with the currentMetaData (which was cached from a previous ColMetaDataToken) and the active transport components.

Row Creation: It instantiates a new StatefulRow(rawRow.getPayload(), currentMetaData, ...).

Segment Wrapping: Because R2DBC expects a stream of varying segments (Rows, Update Counts, Out Parameters), it wraps the StatefulRow into an anonymous Result.RowSegment.

Phase 3: The Reactive Queue (AbstractQueueDrainPublisher)
Because ReactiveResultVisitor extends AbstractQueueDrainPublisher, it inherits a strict reactive backpressure queue.

Queueing: The RowSegment is placed into a thread-safe ConcurrentLinkedQueue via the emit() method.

Emission: The drain loop checks if the downstream client has requested data (Demand > 0). If so, it calls subscriber.onNext(item), pushing the segment across the boundary toward the client.

Phase 4: Client Consumption (TdsResult)
The RowSegment arrives at the user-facing TdsResult class, which manages the thread boundary.

Thread Shift: Inside TdsResult.map(), the segment is intercepted. To prevent user code from blocking the network, the driver dispatches the segment to the Tds-Mapper-Worker thread.

Unwrapping: The worker thread unwraps the Result.RowSegment to expose the underlying StatefulRow.

Client Execution: The driver finally invokes the user's custom lambda (mappingFunction.apply(row, row.getMetadata())), allowing the client application to extract values like row.get(0, Integer.class).

Auto-Draining: Once the user's lambda finishes, the finally block calls ((StatefulRow) row).drain() to clear any unread bytes from the wire and signal the network to fetch the next token.

This pipeline ensures that rows are dynamically materialized "Just-In-Time" as the client demands them, rather than buffering massive result sets in memory.

=====================

To monitor specifically how logical segments (like Metadata and Done tokens) and physical rows flow from the network to your client application, you should focus on the handoffs between the EventLoop, the Queue Publisher, and the Mapper Worker.

Based on your current architecture, here are the specific log signatures to watch for a clear view of the row-by-row lifecycle:

1. The Arrival (EventLoop Thread)
   Watch the ReactiveResultVisitor to see when the driver identifies a new row or protocol segment in the raw network buffer:

Row Detection: [ReactiveResultVisitor] Received RawRowToken. Creating StatefulRow and Emitting.

Metadata Detection: [ReactiveResultVisitor] Received ColMetaDataToken.

Completion Detection: [ReactiveResultVisitor] Received DoneToken. followed by >>> VISITOR: DONE Token parsed successfully!

2. The Internal Queue (Emission)
   The AbstractQueueDrainPublisher shows when a segment is queued for the client and when it is actually pushed across the thread boundary:

Queuing: [AbstractQueueDrain] Emitting item to internal buffer queue.

Dispatching: [AbstractQueueDrain] Pushing item to downstream subscriber.onNext().

3. The Mapping & Draining (Mapper-Worker Thread)
   This is where the user application actually interacts with the row. These logs confirm that the segment has reached the "final destination":

Client Hit: >>> ROW HIT THE MAP FUNCTION! (Logged by your CSharpTdsClientRandom)

Row Cleanup: [StatefulRow] Row fully parsed. Turning EventLoop back on.

Auto-Drain: [StatefulRow] Auto-Drain initiated. Fast-forwarding cursor...

4. Backpressure Guards
   If you see these logs, it tells you exactly why the "flow" has paused:

Network Locked: [StatefulTokenDecoder] ROW token emitted. Protocol-Driven TCP Backpressure invoked.

Network Released: [ReactiveResultVisitor] onResume triggered by demand. Telling Transport to restore OP_READ.

Recommended Log Filter
To see only this flow without the noise of byte-level decoding, you can filter your console to only show lines containing:
RawRowToken|HIT THE MAP|Pushing item|Row fully parsed|DONE Token parsed