Here is the consolidated, comprehensive list of the Networking Thread's responsibilities, incorporating all observations, merging the overlapping concepts, and adding the missing NIO fundamentals.

This is structured as a baseline definition for your driver's architecture.

### 1. TCP Transport & State Management
* **Responsibility:** Ownership of the socket lifecycle, connection health, and negotiated packet state.
* **Description:** The networking thread is the single source of truth for the physical and logical health of the `NioSocketConnection`. It manages the negotiated TDS packet size and all raw I/O operations. Following the "Fail-Fast and LOUD" philosophy, if any discrepancy or unexpected state occurs during a request lifecycle, this thread immediately flags the connection as invalid and closes it to prevent mangled data from affecting the pool.

### 2. Synchronous Login Phase Boundary
* **Responsibility:** Orchestration of the pre-login and login handshakes before asynchronous operations begin.
* **Description:** Before reactive sinks or token queues even exist, the networking thread synchronously manages the Pre-Login and Login7 sequences. It handles raw challenge/response packets to establish the session, negotiate the environment, and verify credentials before opening the gates to the asynchronous query modes.

### 3. TLS / SSL Record Processing
* **Responsibility:** Decryption and encryption of raw network bytes.
* **Description:** For secure connections, the networking thread acts as the TLS gateway. It must pump raw incoming socket bytes through the `SSLEngine` to decrypt them into plaintext before handing them to the protocol framer. Conversely, it must encrypt outgoing payload buffers into TLS records before writing them to the wire.

### 4. Asynchronous Dispatch and Flush Management
* **Responsibility:** Dispatching encoded buffers and managing partial network writes via `OP_WRITE`.
* **Description:** The thread is responsible for taking encoded `ByteBuffer`s and attempting to write them to the socket. Crucially, if the OS network buffer fills up and causes a partial write, this thread must register `OP_WRITE` on the NIO selector. It is responsible for reliably flushing the remaining bytes upon the next selector wakeup before advancing the protocol state.

### 5. Half-Duplex Protocol Enforcement & Iterative Orchestration
* **Responsibility:** Guardian of the strict "Send-then-Receive" sequence across single and multi-packet logical requests.
* **Description:** The networking thread enforces the TDS half-duplex constraint, ensuring a command (or a sequence of sub-commands for large RPC/Batch requests) is fully dispatched before attempting to read responses. It maintains the Entry Guard mutex across these iterations, failing the entire logical request loudly if any sub-request violates the sequence or protocol expectations.

### 6. Logical Response Assembly
* **Responsibility:** Transformation of raw payload bytes into logical TDS tokens and PLP chunks.
* **Description:** Acting as the protocol framer and decoder, the thread strips the 8-byte TDS headers and reassembles fragmented payloads. It converts the continuous stream of raw network bytes into discrete, meaningful logical events—such as `RowToken`, `DoneToken`, or Partial Length Prefixed (PLP) chunks used for streaming Large Objects.

### 7. Data Handoff and Signaling
* **Responsibility:** Enqueuing assembled tokens and notifying the reactive consumer.
* **Description:** As the "Producer," the networking thread places parsed logical units into the strictly bounded `TdsTokenQueue`. Immediately after enqueuing, it fires an available callback to signal the reactive layer that work is ready. The thread's duty ends exactly at notification; it strictly avoids executing downstream application mapping or sink logic.

### 8. Backpressure and Synchronization
* **Responsibility:** Controlling network read interest (`OP_READ`) to match consumer speed.
* **Description:** The thread maintains state-machine alignment with the reactive layer by monitoring the token queue's byte weight. If the queue hits a high-water mark (often due to unconsumed LOBs or slow client processing), the networking thread suspends reading at the OS level. It only resumes `OP_READ` when the consumer drains the queue to a low-water mark.

### 9. Network Timeout and Dead-Peer Detection
* **Responsibility:** Enforcement of read/write deadlines and server unresponsiveness.
* **Description:** If the server goes completely silent after a command is sent, or if a TCP keep-alive fails at the socket layer, the networking thread (or its selector loop) must detect the timeout. In alignment with failing fast, it is responsible for throwing a `TimeoutException` and immediately tearing down the corrupted socket state.