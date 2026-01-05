package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.messages.Message;
import org.tdslib.javatdslib.tokens.done.DoneTokenParser;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeTokenParser;
import org.tdslib.javatdslib.tokens.error.ErrorTokenParser;
import org.tdslib.javatdslib.tokens.info.InfoTokenParser;
import org.tdslib.javatdslib.tokens.loginack.LoginAckTokenParser;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TokenDispatcher {

    private final Map<Byte, TokenParser> parsers = new HashMap<>();

    public TokenDispatcher() {
        register(TokenType.ENV_CHANGE,     new EnvChangeTokenParser());
        register(TokenType.LOGIN_ACK,      new LoginAckTokenParser());
        register(TokenType.ERROR,          new ErrorTokenParser());
        register(TokenType.INFO,           new InfoTokenParser());
        register(TokenType.DONE,           new DoneTokenParser());
        // register more as you implement them
    }

    private void register(TokenType type, TokenParser parser) {
        parsers.put(type.getValue(), parser);
    }

    /**
     * Process all tokens in one single TDS message (one packet's payload).
     */
    public void processMessage(Message message, ConnectionContext context, TokenVisitor visitor) {
        ByteBuffer payload = message.getPayload().duplicate(); // safe working copy

        while (payload.hasRemaining()) {
            byte tokenType = payload.get();

            TokenParser parser = parsers.get(tokenType);
            if (parser == null) {
                // unknown token → skip or throw
                throw new IllegalStateException("No parser for token type 0x" + Integer.toHexString(tokenType & 0xFF));
            }

            // Position is already after type byte → parser consumes the rest
            Token token = parser.parse(payload, tokenType, context);

            // Notify visitor / handler
            visitor.onToken(token);

            // Special handling: reset connection flag
            if (message.isResetConnection()) {
                context.resetToDefaults();
            }
        }
    }
}