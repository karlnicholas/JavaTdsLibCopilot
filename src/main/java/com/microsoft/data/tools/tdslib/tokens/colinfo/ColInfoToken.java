package com.microsoft.data.tools.tdslib.tokens.colinfo;
import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;
public class ColInfoToken extends Token {
    @Override
    public TokenType getType() { return TokenType.COL_INFO; }
    @Override
    public String toString() { return "ColInfoToken"; }
}