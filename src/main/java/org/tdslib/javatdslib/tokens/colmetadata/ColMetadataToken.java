package org.tdslib.javatdslib.tokens.colmetadata;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;
public class ColMetadataToken extends Token {
    @Override
    public TokenType getType() { return TokenType.COL_METADATA; }
    @Override
    public String toString() { return "ColMetadataToken"; }
}