package org.tdslib.javatdslib.query.rpc;
// Immutable key for each binding
record BindingKey(
        BindingType type,
        BindingKind kind,   // which method was used
        String name,        // for NAMED; null otherwise
        int index           // for INDEXED or IMPLIED (1-based); -1 for NAMED
) {
}
