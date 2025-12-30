// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.featureextack;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

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
