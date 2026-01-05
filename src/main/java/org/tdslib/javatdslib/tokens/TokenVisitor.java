package org.tdslib.javatdslib.tokens;

/**
 * Visitor/callback interface that receives each successfully parsed token.
 */
@FunctionalInterface
public interface TokenVisitor {

    /**
     * Called for every token that was successfully parsed from the message payload.
     *
     * @param token the parsed token object (LoginAckToken, EnvChangeToken, ErrorToken, DoneToken, etc.)
     */
    void onToken(Token token);
}