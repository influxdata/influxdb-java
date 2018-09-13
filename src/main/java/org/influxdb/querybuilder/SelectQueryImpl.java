package org.influxdb.querybuilder;

import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.RawTextClause;

public class SelectQueryImpl extends BuiltQuery implements SelectWithSubquery {

  private final SelectCoreImpl<WhereQueryImpl> selectCore;

  SelectQueryImpl(
      final String database,
      final String table,
      final boolean requiresPost,
      final SelectionCoreImpl selectionCore) {
    super(database, requiresPost);
    WhereCoreImpl whereCore = new WhereCoreImpl();
    whereCore.setStatement(this);
    WhereQueryImpl whereQuery = new WhereQueryImpl(whereCore);
    whereQuery.setQuery(this);
    this.selectCore = selectionCore.from(table, whereQuery);
  }

  @Override
  public StringBuilder buildQueryString() {
    return selectCore.buildQueryString();
  }

  @Override
  public StringBuilder buildQueryString(final StringBuilder stringBuilder) {
    return selectCore.buildQueryString(stringBuilder);
  }

  @Override
  public void setSubQuery(QueryStringBuilder query) {
    selectCore.setSubQuery(query);
  }

  @Override
  public WhereQueryImpl where() {
    return selectCore.where();
  }

  @Override
  public WhereQueryImpl where(final Clause clause) {
    return selectCore.where().and(clause);
  }

  @Override
  public WhereQueryImpl where(final String text) {
    return selectCore.where().and(new RawTextClause(text));
  }

  @Override
  public SelectQueryImpl orderBy(final Ordering ordering) {
    selectCore.orderBy(ordering);
    return this;
  }

  @Override
  public SelectQueryImpl groupBy(final Object... columns) {
    selectCore.groupBy(columns);
    return this;
  }

  @Override
  public SelectQueryImpl limit(final int limit) {
    selectCore.limit(limit);
    return this;
  }

  @Override
  public SelectQueryImpl limit(final int limit, final long offSet) {
    selectCore.limit(limit, offSet);
    return this;
  }

  @Override
  public SelectQueryImpl sLimit(final int sLimit) {
    selectCore.sLimit(sLimit);
    return this;
  }

  @Override
  public SelectQueryImpl sLimit(final int sLimit, final long sOffSet) {
    selectCore.sLimit(sLimit, sOffSet);
    return this;
  }

  @Override
  public SelectQueryImpl tz(String timezone) {
    selectCore.tz(timezone);
    return this;
  }
}
