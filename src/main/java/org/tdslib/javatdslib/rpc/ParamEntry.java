package org.tdslib.javatdslib.rpc;

import io.r2dbc.spi.Parameter;

/**
 * Immutable entry for one RPC parameter.
 * Contains everything needed to write TYPE_INFO + value in TDS RPC stream.
 */
public record ParamEntry(
        BindingKey key,               // The original binding key
        Parameter value
) {
}