package org.tdslib.javatdslib;

public interface ConnectionContext {

    // Current state getters (useful for queries too)
    String getCurrentDatabase();
    int getCurrentPacketSize();
    // ... add more later (language, collation, etc.)

    // Mutators for ENVCHANGE and reset
    void setDatabase(String database);
    void setPacketSize(int size);

    /**
     * Resets all session state to defaults as per TDS resetConnection flag.
     * Called automatically when a packet with resetConnection flag is seen.
     */
    void resetToDefaults();

    TdsVersion getTdsVersion();  // ← ADD THIS
}