package org.influxdb.querybuilder;

public class Distinct {

  /** Distinct might as well contain an expression. */
  private final Object expression;

  Distinct(final Object expression) {
    this.expression = expression;
  }

  public Object getExpression() {
    return expression;
  }
}
