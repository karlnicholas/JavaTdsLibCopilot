// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.featureextack;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Feature extension acknowledgement token (skeleton).
 */
public final class FeatureExtAckToken extends Token {
    private final int featureId;

    public FeatureExtAckToken(int featureId) {
        this.featureId = featureId;
    }

    @Override
    public TokenType getType() {
        return TokenType.FEATURE_EXT_ACK;
    }

    public int getFeatureId() {
        return featureId;
    }

    @Override
    public String toString() {
        return "FeatureExtAckToken[FeatureId=" + featureId + "]";
    }
}
