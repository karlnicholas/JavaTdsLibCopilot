package org.tdslib.javatdslib.query.rpc;

import java.util.Objects;
import org.tdslib.javatdslib.protocol.TdsType;

/**
 * Immutable key for each binding.
 *
 * @param type the TDS type of the binding
 * @param name the name of the binding
 */
public record BindingKey(
    TdsType type,
    String name
) {
  /**
   * Compact constructor (implicitly called by the canonical constructor).
   *
   * @param type the TDS type of the binding
   * @param name the name of the binding
   */
  public BindingKey {
    // Enforce non-null for reference types
    name = Objects.requireNonNull(name, "Name must not be null");
  }
}