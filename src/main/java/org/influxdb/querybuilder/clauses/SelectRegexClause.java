package org.influxdb.querybuilder.clauses;

public class SelectRegexClause extends AbstractClause {

  public SelectRegexClause(String clause) {
    super(clause);
  }

  @Override
  public void appendTo(StringBuilder stringBuilder) {
    stringBuilder.append("/").append(name).append("/");
  }
}
