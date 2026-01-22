package org.tdslib.javatdslib.query.rpc;

// Enum for the bind method used
enum BindingKind {
  NAMED,     // bind(String name, ...)
  INDEXED,   // bind(int index, ...)
  IMPLIED    // bind(Object value)
}
