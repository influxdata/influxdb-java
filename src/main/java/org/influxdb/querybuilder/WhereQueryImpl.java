package org.influxdb.querybuilder;

import java.util.List;
import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.ConjunctionClause;

public class WhereQueryImpl<T extends SelectQueryImpl> extends BuiltQuery implements Where, Select {

  private final T query;
  private final WhereCoreImpl whereCore;

  WhereQueryImpl(final T query, final WhereCoreImpl whereCore) {
    super(null);
    this.query = query;
    this.whereCore = whereCore;
  }

  @Override
  public WhereQueryImpl where() {
    return query.where();
  }

  @Override
  public WhereQueryImpl where(Clause clause) {
    return query.where(clause);
  }

  @Override
  public WhereQueryImpl where(String text) {
    return query.where(text);
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
  public WhereNested<WhereQueryImpl<T>> andNested() {
    return new WhereNested(this, false);
  }

  @Override
  public WhereNested<WhereQueryImpl<T>> orNested() {
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
  public SelectQueryImpl fill(final Number value) {
    return query.fill(value);
  }

  @Override
  public SelectQueryImpl fill(final String value) {
    return query.fill(value);
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
  public SelectQueryImpl tz(final String timezone) {
    return query.tz(timezone);
  }

  @Override
  public String getDatabase() {
    return query.getDatabase();
  }

  @Override
  public StringBuilder buildQueryString(StringBuilder stringBuilder) {
    return query.buildQueryString(stringBuilder);
  }

  @Override
  public StringBuilder buildQueryString() {
    return query.buildQueryString();
  }
}
