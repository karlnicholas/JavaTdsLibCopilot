package org.tdslib.javatdslib.tokens;

public interface TdsDecoderSink {
  void onToken(Token token);
  void onColumnData(ColumnData data);
  void onError(Throwable error);
}

// --- Distinct Row Data Classes ---

