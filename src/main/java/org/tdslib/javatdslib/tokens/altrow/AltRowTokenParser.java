package org.tdslib.javatdslib.tokens.altrow;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

public class AltRowTokenParser extends TokenParser {
    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler handler) {
        return new AltRowToken();
    }
}