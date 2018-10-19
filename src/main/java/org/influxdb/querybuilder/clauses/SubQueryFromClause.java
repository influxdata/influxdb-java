package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.QueryStringBuilder;

public class SubQueryFromClause extends FromClause {

  private final QueryStringBuilder queryStringBuilder;

  public SubQueryFromClause(final QueryStringBuilder queryStringBuilder) {
    if (queryStringBuilder == null) {
      throw new IllegalArgumentException("Provide a valid value");
    }
    this.queryStringBuilder = queryStringBuilder;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    stringBuilder.append("(");
    queryStringBuilder.buildQueryString(stringBuilder);
    stringBuilder.append(")");
  }
}
