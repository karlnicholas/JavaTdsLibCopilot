/**
 * The event-driven TDS token parsing system.
 * <p>
 * This package implements a stateless parsing architecture where
 * {@link org.tdslib.javatdslib.tokens.TokenDispatcher} emits tokens to
 * {@link org.tdslib.javatdslib.tokens.TokenVisitor} implementations.
 * <ul>
 * <li><b>models:</b> POJO representations of TDS data fragments.</li>
 * <li><b>parsers:</b> Stateless logic for byte-to-token conversion.</li>
 * </ul>
 */
package org.tdslib.javatdslib.tokens;