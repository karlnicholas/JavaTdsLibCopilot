package org.tdslib.javatdslib.query.rpc;

// Immutable key for each binding
record BindingKey(
    BindingKind kind,   // which method was used
    String name,        // for NAMED; null otherwise
    int index,          // for INDEXED / IMPLIED; -1 for NAMED
    int order           // 1-based position in the bind sequence
) {
}
