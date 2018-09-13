package org.influxdb.querybuilder;

import java.util.List;
import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.ConjunctionClause;

public class WhereSubQueryImpl<T extends SelectWithSubquery, E extends WithSubquery>
    extends SubQuery<E> implements Select, Where {

  private final WhereCoreImpl whereCore;
  private final T selectQuery;

  WhereSubQueryImpl(final T subQuery, final WhereCoreImpl whereCore) {
    this.selectQuery = subQuery;
    this.whereCore = whereCore;
  }

  @Override
  public WhereSubQueryImpl<T, E> and(final Clause clause) {
    whereCore.and(clause);
    return this;
  }

  @Override
  public WhereSubQueryImpl<T, E> or(final Clause clause) {
    whereCore.or(clause);
    return this;
  }

  @Override
  public List<ConjunctionClause> getClauses() {
    return whereCore.getClauses();
  }

  @Override
  public WhereNested<WhereSubQueryImpl<T, E>> andNested() {
    return new WhereNested<>(this, false);
  }

  @Override
  public WhereNested<WhereSubQueryImpl<T, E>> orNested() {
    return new WhereNested<>(this, true);
  }

  @Override
  public T orderBy(final Ordering ordering) {
    return selectQuery.orderBy(ordering);
  }

  @Override
  public T groupBy(final Object... columns) {
    return selectQuery.groupBy(columns);
  }

  @Override
  public T limit(int limit) {
    return selectQuery.limit(limit);
  }

  @Override
  public T limit(int limit, long offSet) {
    return selectQuery.limit(limit, offSet);
  }

  @Override
  public T sLimit(final int sLimit) {
    return selectQuery.sLimit(sLimit);
  }

  @Override
  public T sLimit(final int sLimit, final long sOffSet) {
    return selectQuery.sLimit(sLimit, sOffSet);
  }

  @Override
  public T tz(String timezone) {
    return selectQuery.tz(timezone);
  }

  @Override
  public StringBuilder buildQueryString() {
    return selectQuery.buildQueryString();
  }

  @Override
  public StringBuilder buildQueryString(final StringBuilder stringBuilder) {
    return selectQuery.buildQueryString(stringBuilder);
  }

  @Override
  public WhereSubQueryImpl<T, E> where() {
    return selectQuery.where();
  }

  @Override
  public WhereSubQueryImpl<T, E> where(Clause clause) {
    return selectQuery.where(clause);
  }

  @Override
  public WhereSubQueryImpl<T, E> where(String text) {
    return selectQuery.where(text);
  }
}
