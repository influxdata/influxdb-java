package org.influxdb.querybuilder;

import java.util.ArrayList;
import java.util.List;
import org.influxdb.querybuilder.clauses.AndConjunction;
import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.ConjunctionClause;
import org.influxdb.querybuilder.clauses.OrConjunction;

public class WhereCoreImpl extends SelectDecorator implements Where {

  private final List<ConjunctionClause> clauses = new ArrayList<>();

  @Override
  public WhereCoreImpl and(final Clause clause) {
    clauses.add(new AndConjunction(clause));
    return this;
  }

  @Override
  public WhereCoreImpl or(final Clause clause) {
    clauses.add(new OrConjunction(clause));
    return this;
  }

  @Override
  public List<ConjunctionClause> getClauses() {
    return clauses;
  }

  @Override
  public WhereNested andNested() {
    return new WhereNested(this, false);
  }

  @Override
  public WhereNested orNested() {
    return new WhereNested(this, true);
  }

  @Override
  public SelectCoreImpl orderBy(final Ordering orderings) {
    return statement.orderBy(orderings);
  }

  @Override
  public SelectCoreImpl groupBy(final Object... columns) {
    return statement.groupBy(columns);
  }

  @Override
  public SelectCoreImpl limit(final int limit) {
    return statement.limit(limit);
  }

  @Override
  public SelectCoreImpl limit(final int limit, final long offSet) {
    return statement.limit(limit, offSet);
  }

  @Override
  public SelectCoreImpl sLimit(final int sLimit) {
    return statement.sLimit(sLimit);
  }

  @Override
  public SelectCoreImpl sLimit(final int sLimit, final long sOffSet) {
    return statement.sLimit(sLimit, sOffSet);
  }

  @Override
  public SelectCoreImpl tz(final String timezone) {
    return statement.tz(timezone);
  }
}
