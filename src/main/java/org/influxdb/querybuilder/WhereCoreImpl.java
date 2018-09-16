package org.influxdb.querybuilder;

import java.util.ArrayList;
import java.util.List;
import org.influxdb.querybuilder.clauses.AndConjunction;
import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.ConjunctionClause;
import org.influxdb.querybuilder.clauses.OrConjunction;

public class WhereCoreImpl<T extends Select> implements Select, Where {

  private final List<ConjunctionClause> clauses = new ArrayList<>();

  private final T statement;

  public WhereCoreImpl(T statement) {
    this.statement = statement;
  }

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
  public WhereCoreImpl where() {
    return statement.where();
  }

  @Override
  public WhereCoreImpl where(Clause clause) {
    return statement.where(clause);
  }

  @Override
  public WhereCoreImpl where(String text) {
    return statement.where(text);
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
  public SelectCoreImpl fill(final Number value) {
    return statement.fill(value);
  }

  @Override
  public SelectCoreImpl fill(final String value) {
    return statement.fill(value);
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
