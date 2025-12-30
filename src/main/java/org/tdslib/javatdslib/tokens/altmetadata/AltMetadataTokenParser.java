package org.tdslib.javatdslib.tokens.altmetadata;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;
import java.util.concurrent.CompletableFuture;
public class AltMetadataTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler handler) {
        return CompletableFuture.completedFuture(new AltMetadataToken());
    }
}