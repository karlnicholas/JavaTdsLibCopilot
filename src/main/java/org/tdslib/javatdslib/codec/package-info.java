/**
 * Unified type translation layer between Java types and TDS wire formats.
 * <p>
 * This package provides a symmetric architecture for data conversion:
 * <ul>
 * <li><b>Encoders:</b> Convert Java types to TDS bytes for RPC calls.</li>
 * <li><b>Decoders:</b> Convert raw TDS bytes back to Java types for result sets.</li>
 * </ul>
 * Centralized registries {@link org.tdslib.javatdslib.codec.EncoderRegistry} and
 * {@link org.tdslib.javatdslib.codec.DecoderRegistry} manage these mappings.
 */
package org.tdslib.javatdslib.codec;