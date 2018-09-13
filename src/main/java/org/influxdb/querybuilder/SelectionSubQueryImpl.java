package org.influxdb.querybuilder;

public class SelectionSubQueryImpl<T extends WithSubquery> extends SubQuery<T>
    implements Selection, WithSubquery {

  private final SelectionCoreImpl selectionCore;

  SelectionSubQueryImpl(final T selectQuery) {
    setParent(selectQuery);
    this.selectionCore = new SelectionCoreImpl();
  }

  @Override
  public SelectionSubQueryImpl<T> distinct() {
    selectionCore.distinct();
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> as(final String aliasName) {
    selectionCore.as(aliasName);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> all() {
    selectionCore.all();
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> countAll() {
    selectionCore.countAll();
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> column(final String name) {
    selectionCore.column(name);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> regex(String clause) {
    selectionCore.regex(clause);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> function(final String name, final Object... parameters) {
    selectionCore.function(name, parameters);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> raw(final String text) {
    selectionCore.raw(text);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> count(final Object column) {
    selectionCore.count(column);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> max(final Object column) {
    selectionCore.max(column);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> min(final Object column) {
    selectionCore.min(column);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> sum(final Object column) {
    selectionCore.sum(column);
    return this;
  }

  @Override
  public SelectionSubQueryImpl<T> mean(final Object column) {
    selectionCore.mean(column);
    return this;
  }

  public SelectSubQueryImpl<T> from(final String table) {
    selectionCore.clearSelection();
    SelectSubQueryImpl subSelect =
        new SelectSubQueryImpl<>(table, selectionCore.columns, selectionCore.isDistinct);
    subSelect.setParent(getParent());
    return subSelect;
  }

  public SelectionSubQueryImpl<SelectSubQueryImpl<T>> fromSubQuery() {
    selectionCore.clearSelection();
    SelectSubQueryImpl selectSubQuery =
        new SelectSubQueryImpl(null, selectionCore.columns, selectionCore.isDistinct);
    selectSubQuery.setParent(this.getParent());
    SelectionSubQueryImpl selectionSubQuery = new SelectionSubQueryImpl(selectSubQuery);
    return selectionSubQuery;
  }

  @Override
  public void setSubQuery(QueryStringBuilder query) {
  }

  @Override
  public StringBuilder buildQueryString() {
    return null;
  }

  @Override
  public StringBuilder buildQueryString(StringBuilder stringBuilder) {
    return null;
  }
}
