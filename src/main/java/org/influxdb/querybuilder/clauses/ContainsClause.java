package org.influxdb.querybuilder.clauses;

public class ContainsClause extends RegexClause {

  public ContainsClause(final String name, final String value) {
    super(name, "/" + value + "/");
  }
}
