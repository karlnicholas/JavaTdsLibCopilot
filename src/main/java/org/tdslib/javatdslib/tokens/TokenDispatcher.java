package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.messages.Message;
import org.tdslib.javatdslib.tokens.done.DoneInProcTokenParser;
import org.tdslib.javatdslib.tokens.done.DoneProcTokenParser;
import org.tdslib.javatdslib.tokens.done.DoneTokenParser;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeTokenParser;
import org.tdslib.javatdslib.tokens.error.ErrorTokenParser;
import org.tdslib.javatdslib.tokens.info.InfoTokenParser;
import org.tdslib.javatdslib.tokens.loginack.LoginAckTokenParser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

/**
 * Dispatches parsing of individual TDS tokens from a single message payload.
 * Processes one Message (one packet) at a time — no stream across packets.
 */
public class TokenDispatcher {

    private final Map<Byte, TokenParser> parsers = new HashMap<>();

    public TokenDispatcher() {
        register(TokenType.ENV_CHANGE,     new EnvChangeTokenParser());
        register(TokenType.LOGIN_ACK,      new LoginAckTokenParser());
        register(TokenType.ERROR,          new ErrorTokenParser());
        register(TokenType.INFO,           new InfoTokenParser());
        register(TokenType.DONE,           new DoneTokenParser());
        // Add more parsers as implemented:
         register(TokenType.DONE_IN_PROC, new DoneInProcTokenParser());
         register(TokenType.DONE_PROC,    new DoneProcTokenParser());
        // register(TokenType.COL_METADATA, new ColMetadataTokenParser());
        // etc.
    }

    private void register(TokenType type, TokenParser parser) {
        parsers.put(type.getValue(), parser);
    }

    /**
     * Processes all tokens in a single TDS message (one packet's payload).
     * Calls the visitor for each successfully parsed token.
     *
     * @param message  The TDS packet/message to parse
     * @param context  Connection context for state updates (e.g., packet size, database)
     * @param visitor  Callback to handle each parsed token
     */
    public void processMessage(Message message, ConnectionContext context, TokenVisitor visitor) {
        ByteBuffer payload = message.getPayload().order(ByteOrder.LITTLE_ENDIAN); // safe, independent copy

        while (payload.hasRemaining()) {
            byte tokenTypeByte = payload.get();

            TokenParser parser = parsers.get(tokenTypeByte);
            if (parser == null) {
                // Unknown token: throw or skip (throw is safer during dev)
                throw new IllegalStateException(
                        "No parser registered for token type 0x" + Integer.toHexString(tokenTypeByte & 0xFF));
            }

            // Parser consumes exactly the bytes for this token
            Token token = parser.parse(payload, tokenTypeByte, context);

            // Notify the visitor (caller decides what to do)
            visitor.onToken(token);
        }

        // Handle resetConnection flag after all tokens in this message
        if (message.isResetConnection()) {
            context.resetToDefaults();
        }
    }
}