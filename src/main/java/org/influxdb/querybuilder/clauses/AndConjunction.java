package org.influxdb.querybuilder.clauses;

public class AndConjunction extends ConjunctionClause {

  private static final String AND = "AND";

  public AndConjunction(final Clause clause) {
    super(clause);
  }

  @Override
  public void join(final StringBuilder stringBuilder) {
    stringBuilder.append(" ").append(AND).append(" ");
  }
}
