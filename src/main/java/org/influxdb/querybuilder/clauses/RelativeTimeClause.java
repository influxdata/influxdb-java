package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.time.TimeInterval;

public class RelativeTimeClause extends AbstractClause {

  private final String rule;
  private final TimeInterval timeInterval;

  RelativeTimeClause(final String rule, final TimeInterval timeInterval) {
    super("now()");
    this.rule = rule;
    this.timeInterval = timeInterval;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    stringBuilder.append(name).append(" ").append(rule).append(" ");
    timeInterval.appendTo(stringBuilder);
  }
}
