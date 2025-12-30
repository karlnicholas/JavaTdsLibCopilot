package com.microsoft.data.tools.tdslib.tokens.altmetadata;
import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;
public class AltMetadataToken extends Token {
    @Override
    public TokenType getType() { return TokenType.ALT_METADATA; }
    @Override
    public String toString() { return "AltMetadataToken"; }
}