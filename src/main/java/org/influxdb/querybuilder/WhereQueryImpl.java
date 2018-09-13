package org.influxdb.querybuilder;

import java.util.List;
import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.ConjunctionClause;

public class WhereQueryImpl extends BuiltQueryDecorator<SelectQueryImpl> implements Where {

  private final WhereCoreImpl whereCore;

  WhereQueryImpl(final WhereCoreImpl whereCore) {
    super();
    this.whereCore = whereCore;
  }

  @Override
  public WhereQueryImpl and(final Clause clause) {
    whereCore.and(clause);
    return this;
  }

  @Override
  public WhereQueryImpl or(final Clause clause) {
    whereCore.or(clause);
    return this;
  }

  @Override
  public List<ConjunctionClause> getClauses() {
    return whereCore.getClauses();
  }

  @Override
  public WhereNested<WhereQueryImpl> andNested() {
    return new WhereNested(this, false);
  }

  @Override
  public WhereNested<WhereQueryImpl> orNested() {
    return new WhereNested(this, true);
  }

  @Override
  public SelectQueryImpl orderBy(final Ordering orderings) {
    return query.orderBy(orderings);
  }

  @Override
  public SelectQueryImpl groupBy(final Object... columns) {
    return query.groupBy(columns);
  }

  @Override
  public SelectQueryImpl limit(final int limit) {
    return query.limit(limit);
  }

  @Override
  public SelectQueryImpl limit(final int limit, final long offSet) {
    return query.limit(limit, offSet);
  }

  @Override
  public SelectQueryImpl sLimit(final int sLimit) {
    return query.sLimit(sLimit);
  }

  @Override
  public SelectQueryImpl sLimit(final int sLimit, final long sOffSet) {
    return query.sLimit(sLimit, sOffSet);
  }

  @Override
  public SelectQueryImpl tz(String timezone) {
    return query.tz(timezone);
  }
}
