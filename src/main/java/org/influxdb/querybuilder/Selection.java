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
        if (columnNames == null)
            columnNames = new ArrayList<>();

        columnNames.add(name);
        return this;
    }

    private Selection queueName(Object name) {
        if (previousSelection != null)
            addName(previousSelection);

        previousSelection = name;
        return this;
    }

    public Select.Builder all() {
        if (isDistinct)
            throw new IllegalStateException("DISTINCT function can only be used with one column");
        if (columnNames != null)
            throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));
        if (previousSelection != null)
            throw new IllegalStateException(String.format("Some columns ([%s]) have already been selected.", previousSelection));
        return this;
    }

    public Select.Builder countAll() {
        if (columnNames != null)
            throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));
        if (previousSelection != null)
            throw new IllegalStateException(String.format("Some columns ([%s]) have already been selected.", previousSelection));

        columnNames = COUNT_ALL;
        return this;
    }

    public Selection column(String name) {
        return queueName(name);
    }

    public Selection functionCall(String name, Object... parameters) {
        return queueName(new Function(name, parameters));
    }

    public Selection raw(String rawString) {
        return queueName(new RawString(rawString));
    }

    public Selection count(Object column) {
        return queueName(FunctionFactory.count(column));
    }

    public Selection max(Object column) {
        return queueName(FunctionFactory.max(column));
    }

    public Selection min(Object column) {
        return queueName(FunctionFactory.min(column));
    }

    public Selection sum(Object column) {
        return queueName(FunctionFactory.sum(column));
    }

    public Selection mean(Object column) {
        return queueName(FunctionFactory.mean(column));
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
