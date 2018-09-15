package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.Appender;

public class SimpleClause extends AbstractClause {

  private final String op;
  private final Object value;

  public SimpleClause(final String name, final String op, final Object value) {
    super(name);
    this.op = op;
    this.value = value;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    Appender.appendName(name, stringBuilder).append(" ").append(op).append(" ");
    Appender.appendValue(value, stringBuilder);
  }
}
