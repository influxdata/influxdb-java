package org.influxdb.querybuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.influxdb.querybuilder.clauses.Clause;

public class Select extends BuiltQuery {


    private final String table;
    private final boolean isDistinct;
    private final List<Object> columns;
    private final Where where;
    private Ordering ordering;
    private List<Object> groupByColumns;
    private Integer limit;
    private Long offSet;

    Select(String database,
           String table,
           List<Object> columns,
           boolean isDistinct) {
        super(database);
        this.table = table;
        this.columns = columns;
        this.isDistinct = isDistinct;
        this.where = new Where(this);
    }

    @Override
    StringBuilder buildQueryString() {
        StringBuilder builder = new StringBuilder();

        builder.append("SELECT ");

        if (isDistinct)
            if (columns.size() > 1) {
                throw new IllegalStateException("DISTINCT function can only be used with one column");
            }

        if (columns == null) {
            builder.append('*');
        } else {
            Appender.joinAndAppendNames(builder, columns);
        }
        builder.append(" FROM ");

        Appender.appendName(table, builder);

        if (!where.clauses.isEmpty()) {
            builder.append(" WHERE ");
            Appender.joinAndAppend(builder, " AND ", where.clauses);
        }

        if (groupByColumns != null) {
            builder.append(" GROUP BY ");
            Appender.joinAndAppendNames(builder, groupByColumns);
        }

        if (ordering != null) {
            builder.append(" ORDER BY ");
            Appender.joinAndAppend(builder, ",", Collections.singletonList(ordering));
        }

        if (limit != null) {
            builder.append(" LIMIT ").append(limit);
        }

        if (offSet != null) {
            builder.append(" OFFSET ").append(offSet);
        }

        return builder;
    }


    public Where where(Clause clause) {
        return where.and(clause);
    }

    public Where where() {
        return where;
    }

    public Select orderBy(Ordering ordering) {

        this.ordering = ordering;
        return this;
    }

    public Select groupBy(Object... columns) {
        this.groupByColumns = Arrays.asList(columns);
        return this;
    }

    public Select limit(int limit) {
        if (limit <= 0)
            throw new IllegalArgumentException("Invalid LIMIT value, must be strictly positive");

        if (this.limit != null)
            throw new IllegalStateException("A LIMIT value has already been provided");

        this.limit = limit;
        return this;
    }

    public Select limit(int limit, long offSet) {
        if (limit <= 0|| offSet<=0)
            throw new IllegalArgumentException("Invalid LIMIT and OFFSET Value, must be strictly positive");

        this.limit = limit;
        this.offSet = offSet;
        return this;
    }

    public static class Where extends BuiltQueryDecorator<Select> {

        private final List<Clause> clauses = new ArrayList<Clause>();

        Where(Select statement) {
            super(statement);
        }

        public Where and(Clause clause) {
            clauses.add(clause);
            return this;
        }

        public Select orderBy(Ordering orderings) {
            return query.orderBy(orderings);
        }

        public Select groupBy(Object... columns) {
            return query.groupBy(columns);
        }

        public Select limit(int limit) {
            return query.limit(limit);
        }

        public Select limit(int limit, long offSet) {
            return query.limit(limit,offSet);
        }
    }

    public static class Builder {

        List<Object> columnNames;
        boolean isDistinct;

        Builder() {
        }

        public Builder(List<Object> columnNames) {
            this.columnNames = columnNames;
        }

        public Select from(String table) {
            return from(null, table);
        }

        public Select from(String database, String table) {
            return new Select(database, table, columnNames, isDistinct);
        }

    }

}
