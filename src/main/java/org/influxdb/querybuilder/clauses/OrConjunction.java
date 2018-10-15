package org.influxdb.querybuilder.clauses;

public class OrConjunction extends ConjunctionClause {

  private static final String OR = "OR";

  public OrConjunction(final Clause clause) {
    super(clause);
  }

  @Override
  public void join(final StringBuilder stringBuilder) {
    stringBuilder.append(" ").append(OR).append(" ");
  }
}
