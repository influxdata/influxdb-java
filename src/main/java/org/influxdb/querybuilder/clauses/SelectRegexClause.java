package org.influxdb.querybuilder.clauses;

public class SelectRegexClause extends AbstractClause {

  public SelectRegexClause(final String clause) {
    super(clause);
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    stringBuilder.append(name);
  }
}
