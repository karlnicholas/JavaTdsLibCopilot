package com.microsoft.data.tools.tdslib.payloads.prelogin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a list of PreLoginPayloadToken objects.
 */
public class PreLoginPayloadTokenList implements Iterable<PreLoginPayloadToken> {
    private final List<PreLoginPayloadToken> tokens;

    public PreLoginPayloadTokenList() {
        this.tokens = new ArrayList<>();
    }

    public void add(PreLoginPayloadToken token) {
        tokens.add(token);
    }

    public PreLoginPayloadToken get(int index) {
        return tokens.get(index);
    }

    public int size() {
        return tokens.size();
    }

    public List<PreLoginPayloadToken> asUnmodifiableList() {
        return Collections.unmodifiableList(tokens);
    }

    @Override
    public Iterator<PreLoginPayloadToken> iterator() {
        return tokens.iterator();
    }
}
