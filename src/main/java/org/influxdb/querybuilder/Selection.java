package org.influxdb.querybuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Selection extends Select.Builder {

    private static final List<Object> COUNT_ALL = Collections.singletonList(new Function("count", new RawString("*")));

    private Object previousSelection;

    public Selection distinct() {
        this.isDistinct = true;
        assert previousSelection != null;
        Object distinct = new Distinct(previousSelection);
        previousSelection = null;
        return addName(distinct);
    }

    public Selection as(String aliasName) {
        assert previousSelection != null;
        Object alias = new Alias(previousSelection, aliasName);
        previousSelection = null;
        return addName(alias);
    }

    private Selection addName(Object name) {
        if (columns == null)
            columns = new ArrayList<>();

        columns.add(name);
        return this;
    }

    private Selection addToColumns(Object name) {
        if (previousSelection != null)
            addName(previousSelection);

        previousSelection = name;
        return this;
    }

    public Select.Builder all() {
        if (isDistinct)
            throw new IllegalStateException("DISTINCT function can only be used with one column");
        if (columns != null)
            throw new IllegalStateException("Can't select all columns over columns selected previously");
        if (previousSelection != null)
            throw new IllegalStateException("Can't select all columns over columns selected previously");
        return this;
    }

    public Select.Builder countAll() {
        if (columns != null)
            throw new IllegalStateException("Can't select all columns over columns selected previously");
        if (previousSelection != null)
            throw new IllegalStateException("Can't select all columns over columns selected previously");

        columns = COUNT_ALL;
        return this;
    }

    public Selection column(String name) {
        return addToColumns(name);
    }

    public Selection function(String name, Object... parameters) {
        return addToColumns(FunctionFactory.function(name, parameters));
    }

    public Selection raw(String rawString) {
        return addToColumns(new RawString(rawString));
    }

    public Selection count(Object column) {
        return addToColumns(FunctionFactory.count(column));
    }

    public Selection max(Object column) {
        return addToColumns(FunctionFactory.max(column));
    }

    public Selection min(Object column) {
        return addToColumns(FunctionFactory.min(column));
    }

    public Selection sum(Object column) {
        return addToColumns(FunctionFactory.sum(column));
    }

    public Selection mean(Object column) {
        return addToColumns(FunctionFactory.mean(column));
    }

    @Override
    public Select from(String keyspace, String table) {
        if (previousSelection != null)
            addName(previousSelection);
        previousSelection = null;
        return super.from(keyspace, table);
    }

    @Override
    public Select from(String table) {
        if (previousSelection != null)
            addName(previousSelection);
        previousSelection = null;
        return super.from(table);
    }

}
