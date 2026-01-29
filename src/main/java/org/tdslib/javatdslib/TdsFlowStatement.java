package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 TDS implementation of R2DBC Statement for FLOW queries.
 */
public class TdsFlowStatement implements Statement {
  private final TdsMessage queryMessage;
  private final TdsTransport transport;
  public TdsFlowStatement(TdsMessage queryMessage, TdsTransport transport) {
    this.queryMessage = queryMessage;
    this.transport = transport;
  }
  @Override
  public Statement add() {
    return null;
  }

  @Override
  public Statement bind(int i, Object o) {
    return null;
  }

  @Override
  public Statement bind(String s, Object o) {
    return null;
  }

  @Override
  public Statement bindNull(int i, Class<?> aClass) {
    return null;
  }

  @Override
  public Statement bindNull(String s, Class<?> aClass) {
    return null;
  }

  @Override
  public Publisher<Result> execute() {
    // We return a Publisher that emits a single FlowResult
    return subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {
          if (n > 0) {
            subscriber.onNext(new TdsFlowResult(queryMessage, transport));
            subscriber.onComplete();
          }
        }
        @Override
        public void cancel() {}
      });
    };
  }

  @Override
  public Statement returnGeneratedValues(String... columns) {
    return Statement.super.returnGeneratedValues(columns);
  }

  @Override
  public Statement fetchSize(int rows) {
    return Statement.super.fetchSize(rows);
  }
}