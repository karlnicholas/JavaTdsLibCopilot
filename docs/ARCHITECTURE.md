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