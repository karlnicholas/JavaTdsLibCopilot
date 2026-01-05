package org.tdslib.javatdslib.tokens.colinfo;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

public class ColInfoTokenParser extends TokenParser {
    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler handler) {
        return new ColInfoToken();
    }
}