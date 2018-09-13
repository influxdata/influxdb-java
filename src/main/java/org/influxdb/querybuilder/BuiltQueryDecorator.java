package org.influxdb.querybuilder;

import org.influxdb.querybuilder.clauses.Clause;

public abstract class BuiltQueryDecorator<T extends SelectQueryImpl> extends BuiltQuery
    implements Select {

  SelectQueryImpl query;

  public BuiltQueryDecorator() {
    super(null);
  }

  public void setQuery(final T query) {
    this.query = query;
  }

  @Override
  public String getCommand() {
    return query.getCommand();
  }

  @Override
  public StringBuilder buildQueryString() {
    return query.buildQueryString();
  }

  @Override
  public StringBuilder buildQueryString(final StringBuilder stringBuilder) {
    return query.buildQueryString(stringBuilder);
  }

  @Override
  public String getDatabase() {
    return query.getDatabase();
  }

  @Override
  public Where where() {
    return query.where();
  }

  @Override
  public Where where(final Clause clause) {
    return query.where(clause);
  }

  @Override
  public Where where(final String text) {
    return query.where(text);
  }

  @Override
  public SelectQueryImpl orderBy(final Ordering ordering) {
    return query.orderBy(ordering);
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
}
