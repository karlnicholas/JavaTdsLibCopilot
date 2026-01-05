package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the result of a Login7 request.
 * Collects success/failure status and any side-effects (environment changes, errors).
 */
public class LoginResponse {

    private boolean success = false;
    private String errorMessage = null;
    private String database = null;

    private final List<EnvChangeToken> envChanges = new ArrayList<>();

    // --- Mutators (used during token processing) ---

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        this.success = false; // error implies failure
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void addEnvChange(EnvChangeToken change) {
        if (change != null) {
            envChanges.add(change);
        }
    }

    // --- Accessors ---

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getDatabase() {
        return database;
    }

    /**
     * Returns an unmodifiable view of the collected environment changes.
     */
    public List<EnvChangeToken> getEnvChanges() {
        return Collections.unmodifiableList(envChanges);
    }

    /**
     * Convenience: returns true if any ENVCHANGE tokens were received.
     */
    public boolean hasEnvChanges() {
        return !envChanges.isEmpty();
    }

    @Override
    public String toString() {
        return "LoginResponse{" +
                "success=" + success +
                ", errorMessage='" + errorMessage + '\'' +
                ", database='" + database + '\'' +
                ", envChanges=" + envChanges.size() + " item(s)" +
                '}';
    }
}