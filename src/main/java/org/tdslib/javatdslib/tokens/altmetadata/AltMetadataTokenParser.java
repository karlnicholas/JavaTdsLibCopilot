package org.tdslib.javatdslib.tokens.altmetadata;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

public class AltMetadataTokenParser extends TokenParser {
    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler handler) {
        return new AltMetadataToken();
    }
}