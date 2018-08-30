package org.influxdb.querybuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Selection extends Select.Builder {

    private static final List<Object> COUNT_ALL = Collections.singletonList(new Function("COUNT", new RawString("*")));

    private Object currentSelection;

    public Selection distinct() {
        assertColumnIsSelected();
        this.isDistinct = true;
        Object distinct = new Distinct(currentSelection);
        currentSelection = null;
        return moveToColumns(distinct);
    }

    public Selection as(String aliasName) {
        assertColumnIsSelected();
        Object alias = new Alias(currentSelection, aliasName);
        currentSelection = null;
        return moveToColumns(alias);
    }

    private void assertColumnIsSelected() {
        if(currentSelection == null) {
            throw new IllegalStateException("You need to select a column prior to calling distinct");
        }
    }

    private Selection moveToColumns(Object name) {
        if (columns == null)
            columns = new ArrayList<>();

        columns.add(name);
        return this;
    }

    private Selection addToCurrentColumn(Object name) {
        if (currentSelection != null)
            moveToColumns(currentSelection);

        currentSelection = name;
        return this;
    }

    public Select.Builder all() {
        if (isDistinct)
            throw new IllegalStateException("DISTINCT function can only be used with one column");
        if (columns != null)
            throw new IllegalStateException("Can't select all columns over columns selected previously");
        if (currentSelection != null)
            throw new IllegalStateException("Can't select all columns over columns selected previously");
        return this;
    }

    public Select.Builder countAll() {
        if (columns != null)
            throw new IllegalStateException("Can't select all columns over columns selected previously");
        if (currentSelection != null)
            throw new IllegalStateException("Can't select all columns over columns selected previously");

        columns = COUNT_ALL;
        return this;
    }

    public Selection column(String name) {
        return addToCurrentColumn(name);
    }

    public Selection function(String name, Object... parameters) {
        return addToCurrentColumn(FunctionFactory.function(name, parameters));
    }

    public Selection raw(String rawString) {
        return addToCurrentColumn(new RawString(rawString));
    }

    public Selection count(Object column) {
        return addToCurrentColumn(FunctionFactory.count(column));
    }

    public Selection max(Object column) {
        return addToCurrentColumn(FunctionFactory.max(column));
    }

    public Selection min(Object column) {
        return addToCurrentColumn(FunctionFactory.min(column));
    }

    public Selection sum(Object column) {
        return addToCurrentColumn(FunctionFactory.sum(column));
    }

    public Selection mean(Object column) {
        return addToCurrentColumn(FunctionFactory.mean(column));
    }

    @Override
    public Select from(String keyspace, String table) {
        if (currentSelection != null)
            moveToColumns(currentSelection);
        currentSelection = null;
        return super.from(keyspace, table);
    }

    @Override
    public Select from(String table) {
        if (currentSelection != null)
            moveToColumns(currentSelection);
        currentSelection = null;
        return super.from(table);
    }

}
