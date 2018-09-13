package org.influxdb.querybuilder;

import java.util.List;
import org.influxdb.querybuilder.clauses.Clause;

public class SelectSubQueryImpl<T extends WithSubquery> extends SubQuery<T>
    implements SelectWithSubquery {

  private SelectCoreImpl<WhereSubQueryImpl<SelectSubQueryImpl<T>, T>> selectCore;
  private WhereSubQueryImpl<SelectSubQueryImpl<T>, T> whereSubQuery;

  SelectSubQueryImpl(final String table, final List<Object> columns, final boolean isDistinct) {
    whereSubQuery = new WhereSubQueryImpl<>(this, new WhereCoreImpl());
    this.selectCore = new SelectCoreImpl<>(table, columns, isDistinct, whereSubQuery);
  }

  @Override
  public WhereSubQueryImpl<SelectSubQueryImpl<T>, T> where() {
    return selectCore.where();
  }

  @Override
  public WhereSubQueryImpl<SelectSubQueryImpl<T>, T> where(final Clause clause) {
    return selectCore.where(clause);
  }

  @Override
  public WhereSubQueryImpl<SelectSubQueryImpl<T>, T> where(final String text) {
    return selectCore.where(text);
  }

  @Override
  public SelectSubQueryImpl<T> orderBy(final Ordering ordering) {
    selectCore.orderBy(ordering);
    return this;
  }

  @Override
  public SelectSubQueryImpl<T> groupBy(final Object... columns) {
    selectCore.groupBy(columns);
    return this;
  }

  @Override
  public SelectSubQueryImpl<T> limit(final int limit) {
    selectCore.limit(limit);
    return this;
  }

  @Override
  public SelectSubQueryImpl<T> limit(final int limit, final long offSet) {
    selectCore.limit(limit, offSet);
    return this;
  }

  @Override
  public SelectSubQueryImpl<T> sLimit(final int sLimit) {
    selectCore.sLimit(sLimit);
    return this;
  }

  @Override
  public SelectSubQueryImpl<T> sLimit(final int sLimit, final long sOffSet) {
    selectCore.sLimit(sLimit, sOffSet);
    return this;
  }

  @Override
  public SelectSubQueryImpl<T> tz(final String timezone) {
    selectCore.tz(timezone);
    return this;
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
  void setParent(T parent) {
    whereSubQuery.setParent(parent);
    super.setParent(parent);
  }
}
