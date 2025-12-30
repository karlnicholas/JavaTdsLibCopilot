package org.tdslib.javatdslib.tokens.colinfo;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;
public class ColInfoToken extends Token {
    @Override
    public TokenType getType() { return TokenType.COL_INFO; }
    @Override
    public String toString() { return "ColInfoToken"; }
}