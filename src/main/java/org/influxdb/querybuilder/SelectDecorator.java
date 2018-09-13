package org.influxdb.querybuilder;

import org.influxdb.querybuilder.clauses.Clause;

public abstract class SelectDecorator<T extends Select, E extends Where> implements Select {

  T statement;

  /**
   * builder
   *
   * @param statement
   */
  public void setStatement(final T statement) {
    this.statement = statement;
  }

  @Override
  public E where() {
    return statement.where();
  }

  @Override
  public E where(final Clause clause) {
    return statement.where(clause);
  }

  @Override
  public E where(final String text) {
    return statement.where(text);
  }

  @Override
  public T orderBy(final Ordering ordering) {
    return statement.orderBy(ordering);
  }

  @Override
  public T groupBy(final Object... columns) {
    return statement.groupBy(columns);
  }

  @Override
  public T limit(final int limit) {
    return statement.limit(limit);
  }

  @Override
  public T limit(final int limit, final long offSet) {
    return statement.limit(limit, offSet);
  }
}
