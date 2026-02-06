package org.tdslib.javatdslib.query.rpc;

/**
 * Immutable entry for one RPC parameter.
 * Contains everything needed to write TYPE_INFO + value in TDS RPC stream.
 */
public record ParamEntry(
        BindingKey key,               // The original binding key
        Object value
) {
}