package com.microsoft.data.tools.tdslib.tokens.altrow;
import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenParser;
import com.microsoft.data.tools.tdslib.tokens.TokenStreamHandler;
import com.microsoft.data.tools.tdslib.tokens.TokenType;
import java.util.concurrent.CompletableFuture;
public class AltRowTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler handler) {
        return CompletableFuture.completedFuture(new AltRowToken());
    }
}