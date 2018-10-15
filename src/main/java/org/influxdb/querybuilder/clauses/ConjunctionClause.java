package org.influxdb.querybuilder.clauses;

public abstract class ConjunctionClause implements Conjunction, Clause {

  private Clause clause;

  public ConjunctionClause(final Clause clause) {
    this.clause = clause;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    clause.appendTo(stringBuilder);
  }
}
