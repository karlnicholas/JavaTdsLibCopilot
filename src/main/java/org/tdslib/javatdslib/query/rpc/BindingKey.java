package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.TdsType;

import java.util.Objects;

// Immutable key for each binding
public record BindingKey(
        TdsType type,
        String name
) {
  // Compact constructor (implicitly called by the canonical constructor)
  public BindingKey {
    // Enforce non-null for reference types
    name = Objects.requireNonNull(name, "Name must not be null");
  }
}
