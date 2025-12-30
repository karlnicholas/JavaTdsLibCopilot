package com.microsoft.data.tools.tdslib.tokens.altrow;
import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;
public class AltRowToken extends Token {
    @Override
    public TokenType getType() { return TokenType.ALT_ROW; }
    @Override
    public String toString() { return "AltRowToken"; }
}