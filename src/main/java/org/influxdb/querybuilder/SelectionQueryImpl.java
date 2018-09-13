package org.influxdb.querybuilder;

import java.util.ArrayList;

public class SelectionQueryImpl implements Selection {

  private final SelectionCoreImpl selectionCore;
  private boolean requiresPost;

  SelectionQueryImpl(final SelectionCoreImpl selectionCore) {
    this.selectionCore = selectionCore;
  }

  @Override
  public SelectionQueryImpl distinct() {
    selectionCore.distinct();
    return this;
  }

  public SelectionQueryImpl requiresPost() {
    requiresPost = true;
    return this;
  }

  @Override
  public SelectionQueryImpl as(final String aliasName) {
    selectionCore.as(aliasName);
    return this;
  }

  @Override
  public SelectionQueryImpl all() {
    selectionCore.all();
    return this;
  }

  @Override
  public SelectionQueryImpl countAll() {
    selectionCore.countAll();
    return this;
  }

  @Override
  public SelectionQueryImpl column(final String name) {
    selectionCore.column(name);
    return this;
  }

  @Override
  public SelectionQueryImpl regex(String clause) {
    selectionCore.regex(clause);
    return this;
  }

  @Override
  public SelectionQueryImpl function(final String name, final Object... parameters) {
    selectionCore.function(name, parameters);
    return this;
  }

  @Override
  public SelectionQueryImpl raw(final String text) {
    selectionCore.raw(text);
    return this;
  }

  @Override
  public SelectionQueryImpl count(final Object column) {
    selectionCore.count(column);
    return this;
  }

  @Override
  public SelectionQueryImpl max(final Object column) {
    selectionCore.max(column);
    return this;
  }

  @Override
  public SelectionQueryImpl min(final Object column) {
    selectionCore.min(column);
    return this;
  }

  @Override
  public SelectionQueryImpl sum(final Object column) {
    selectionCore.sum(column);
    return this;
  }

  @Override
  public SelectionQueryImpl mean(final Object column) {
    selectionCore.mean(column);
    return this;
  }

  public SelectQueryImpl from(final String database, final String table) {
    SelectQueryImpl selectQuery = new SelectQueryImpl(database, table, requiresPost, selectionCore);
    return selectQuery;
  }

  public SelectSubQueryImpl<SelectQueryImpl> fromSubQuery(
      final String database, final String table) {
    SelectSubQueryImpl<SelectQueryImpl> subSelect =
        new SelectSubQueryImpl<>(table, new ArrayList<>(), selectionCore.isDistinct);
    subSelect.setParent(from(database, null));
    return subSelect;
  }

  public SelectionSubQueryImpl<SelectQueryImpl> fromSubQuery(String database) {
    SelectQueryImpl selectQuery = from(database, null);
    return new SelectionSubQueryImpl(selectQuery);
  }
}
