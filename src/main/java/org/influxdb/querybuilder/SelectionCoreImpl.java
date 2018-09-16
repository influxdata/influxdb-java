package org.influxdb.querybuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.influxdb.querybuilder.clauses.*;

class SelectionCoreImpl implements Selection, WithInto {

  protected List<Object> columns;
  protected boolean isDistinct;
  private String intoMeasurement;
  private static final List<Object> COUNT_ALL =
      Collections.singletonList(new Function("COUNT", new RawText("*")));

  private Object currentSelection;

  SelectionCoreImpl() {
  }

  SelectionCoreImpl(final Object[] columns) {
    for (Object column : columns) {
      addToCurrentColumn(column);
    }
  }

  @Override
  public Selection distinct() {
    assertColumnIsSelected();
    this.isDistinct = true;
    Object distinct = new Distinct(currentSelection);
    currentSelection = null;
    return moveToColumns(distinct);
  }

  @Override
  public Selection as(final String aliasName) {
    assertColumnIsSelected();
    Object alias = new Alias(currentSelection, aliasName);
    currentSelection = null;
    return moveToColumns(alias);
  }

  private void assertColumnIsSelected() {
    if (currentSelection == null) {
      throw new IllegalStateException("You need to select a column prior to calling distinct");
    }
  }

  private SelectionCoreImpl moveToColumns(final Object name) {
    if (columns == null) {
      columns = new ArrayList<>();
    }

    columns.add(name);
    return this;
  }

  private SelectionCoreImpl addToCurrentColumn(final Object name) {
    if (currentSelection != null) {
      moveToColumns(currentSelection);
    }

    currentSelection = name;
    return this;
  }

  @Override
  public SelectionCoreImpl all() {
    if (isDistinct) {
      throw new IllegalStateException("DISTINCT function can only be used with one column");
    }
    if (columns != null) {
      throw new IllegalStateException("Can't select all columns over columns selected previously");
    }
    if (currentSelection != null) {
      throw new IllegalStateException("Can't select all columns over columns selected previously");
    }
    return this;
  }

  @Override
  public SelectionCoreImpl countAll() {
    if (columns != null) {
      throw new IllegalStateException("Can't select all columns over columns selected previously");
    }
    if (currentSelection != null) {
      throw new IllegalStateException("Can't select all columns over columns selected previously");
    }
    columns = COUNT_ALL;
    return this;
  }

  @Override
  public SelectionCoreImpl column(final String name) {
    return addToCurrentColumn(name);
  }

  @Override
  public SelectionCoreImpl regex(final String clause) {
    return addToCurrentColumn(new SelectRegexClause(clause));
  }

  @Override
  public SelectionCoreImpl function(final String name, final Object... parameters) {
    return addToCurrentColumn(FunctionFactory.function(name, parameters));
  }

  @Override
  public SelectionCoreImpl raw(final String text) {
    return addToCurrentColumn(new RawText(text));
  }

  @Override
  public SelectionCoreImpl count(final Object column) {
    return addToCurrentColumn(FunctionFactory.count(column));
  }

  @Override
  public SelectionCoreImpl max(final Object column) {
    return addToCurrentColumn(FunctionFactory.max(column));
  }

  @Override
  public SelectionCoreImpl min(final Object column) {
    return addToCurrentColumn(FunctionFactory.min(column));
  }

  @Override
  public SelectionCoreImpl sum(final Object column) {
    return addToCurrentColumn(FunctionFactory.sum(column));
  }

  @Override
  public SelectionCoreImpl mean(final Object column) {
    return addToCurrentColumn(FunctionFactory.mean(column));
  }

  @Override
  public SelectionCoreImpl into(final String measurement) {
    this.intoMeasurement = measurement;
    return this;
  }

  @Override
  public Selection op(final OperationClause operationClause) {
    return addToCurrentColumn(operationClause);
  }

  @Override
  public Selection op(final Object arg1, final String op, final Object arg2) {
    return addToCurrentColumn(new OperationClause(arg1, op, arg2));
  }

  @Override
  public Selection cop(final SimpleClause simpleClause) {
    return addToCurrentColumn(simpleClause);
  }

  @Override
  public Selection cop(final String column, final String op, final Object arg2) {
    return addToCurrentColumn(new SimpleClause(column, op, arg2));
  }

  <E extends Where> SelectCoreImpl<E> from(final FromClause fromClause, final E where) {
    clearSelection();
    return new SelectCoreImpl(fromClause, columns, isDistinct, where, intoMeasurement);
  }

  <E extends Where> SelectCoreImpl<E> from(final E where) {
    clearSelection();
    return new SelectCoreImpl(columns, isDistinct, where, intoMeasurement);
  }

  protected SelectionCoreImpl clearSelection() {
    if (currentSelection != null) {
      moveToColumns(currentSelection);
    }
    currentSelection = null;
    return this;
  }
}
