package org.tdslib.javatdslib.tokens.altrow;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;
public class AltRowToken extends Token {
    @Override
    public TokenType getType() { return TokenType.ALT_ROW; }
    @Override
    public String toString() { return "AltRowToken"; }
}