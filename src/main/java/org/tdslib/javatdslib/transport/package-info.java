/**
 * Wire-level network I/O and packet construction.
 * <p>
 * Manages the physical connection context and the assembly of TDS packets
 * from raw network buffers. Includes specialized builders
 * for SQL queries and RPC calls.
 */
package org.tdslib.javatdslib.transport;