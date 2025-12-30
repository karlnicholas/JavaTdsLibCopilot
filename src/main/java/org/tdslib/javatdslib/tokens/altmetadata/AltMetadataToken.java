package org.tdslib.javatdslib.tokens.altmetadata;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;
public class AltMetadataToken extends Token {
    @Override
    public TokenType getType() { return TokenType.ALT_METADATA; }
    @Override
    public String toString() { return "AltMetadataToken"; }
}