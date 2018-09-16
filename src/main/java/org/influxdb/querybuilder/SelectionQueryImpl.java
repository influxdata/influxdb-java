package org.influxdb.querybuilder;

import java.util.ArrayList;
import java.util.Arrays;
import org.influxdb.querybuilder.clauses.*;

public class SelectionQueryImpl implements Selection, WithInto {

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
  public SelectionQueryImpl regex(final String clause) {
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

  @Override
  public SelectionQueryImpl into(final String measurement) {
    selectionCore.into(measurement);
    return this;
  }

  @Override
  public SelectionQueryImpl op(final OperationClause operationClause) {
    selectionCore.op(operationClause);
    return this;
  }

  @Override
  public SelectionQueryImpl op(final Object arg1, final String op, final Object arg2) {
    selectionCore.op(arg1, op, arg2);
    return this;
  }

  @Override
  public SelectionQueryImpl cop(final SimpleClause simpleClause) {
    selectionCore.cop(simpleClause);
    return this;
  }

  @Override
  public SelectionQueryImpl cop(final String column, final String op, final Object arg2) {
    selectionCore.cop(column, op, arg2);
    return this;
  }

  public SelectQueryImpl from(final String database, final String table) {
    SelectQueryImpl selectQuery =
        new SelectQueryImpl(database, new SimpleFromClause(table), requiresPost, selectionCore);
    return selectQuery;
  }

  public SelectQueryImpl from(final String database, final String[] table) {
    if (table == null) {
      throw new IllegalArgumentException("Tables names should be specified");
    }
    SelectQueryImpl selectQuery =
        new SelectQueryImpl(
            database, new MultipleFromClause(Arrays.asList(table)), requiresPost, selectionCore);
    return selectQuery;
  }

  public SelectQueryImpl fromRaw(final String database, final String text) {
    SelectQueryImpl selectQuery =
        new SelectQueryImpl(database, new RawFromClause(text), requiresPost, selectionCore);
    return selectQuery;
  }

  public SelectQueryImpl from(final String database) {
    SelectQueryImpl selectQuery = new SelectQueryImpl(database, requiresPost, selectionCore);
    return selectQuery;
  }

  public SelectSubQueryImpl<SelectQueryImpl> fromSubQuery(
      final String database, final String table) {
    SelectSubQueryImpl<SelectQueryImpl> subSelect =
        new SelectSubQueryImpl<>(
            new SimpleFromClause(table), new ArrayList<>(), selectionCore.isDistinct);
    subSelect.setParent(from(database));
    return subSelect;
  }

  public SelectSubQueryImpl<SelectQueryImpl> fromSubQuery(
      final String database, final String[] tables) {
    SelectSubQueryImpl<SelectQueryImpl> subSelect =
        new SelectSubQueryImpl<>(
            new MultipleFromClause(Arrays.asList(tables)),
            new ArrayList<>(),
            selectionCore.isDistinct);
    subSelect.setParent(from(database));
    return subSelect;
  }

  public SelectSubQueryImpl<SelectQueryImpl> fromSubQueryRaw(
      final String database, final String text) {
    SelectSubQueryImpl<SelectQueryImpl> subSelect =
        new SelectSubQueryImpl<>(
            new RawFromClause(text), new ArrayList<>(), selectionCore.isDistinct);
    subSelect.setParent(from(database));
    return subSelect;
  }

  public SelectionSubQueryImpl<SelectQueryImpl> fromSubQuery(final String database) {
    SelectQueryImpl selectQuery = from(database);
    return new SelectionSubQueryImpl(selectQuery);
  }
}
