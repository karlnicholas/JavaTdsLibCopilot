package com.microsoft.data.tools.tdslib.tokens.colmetadata;
import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;
public class ColMetadataToken extends Token {
    @Override
    public TokenType getType() { return TokenType.COL_METADATA; }
    @Override
    public String toString() { return "ColMetadataToken"; }
}