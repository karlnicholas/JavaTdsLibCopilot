package org.tdslib.javatdslib.query.rpc;

import java.util.Objects;

// Immutable key for each binding
record BindingKey(
        BindingType type,
        String name
) {
  // Compact constructor (implicitly called by the canonical constructor)
  public BindingKey {
    // Enforce non-null for reference types
    name = Objects.requireNonNull(name, "Name must not be null");
  }
}
