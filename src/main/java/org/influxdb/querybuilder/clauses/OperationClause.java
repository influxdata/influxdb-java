package org.influxdb.querybuilder.clauses;

import static org.influxdb.querybuilder.Appender.appendValue;

public class OperationClause extends AbstractClause {

  private final Object arg1;
  private final String op;
  private final Object arg2;

  public OperationClause(final Object arg1, final String op, final Object arg2) {
    super(null);
    this.arg1 = arg1;
    this.op = op;
    this.arg2 = arg2;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    appendArg(arg1, stringBuilder);
    stringBuilder.append(" ").append(op).append(" ");
    appendArg(arg2, stringBuilder);
  }

  private void appendArg(final Object arg, final StringBuilder stringBuilder) {
    if (arg instanceof OperationClause || arg instanceof SimpleClause) {
      stringBuilder.append("(");
      appendValue(arg, stringBuilder);
      stringBuilder.append(")");
    } else {
      appendValue(arg, stringBuilder);
    }
  }
}
