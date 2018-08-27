package org.influxdb.querybuilder;

public class BuiltQueryDecorator<T extends BuiltQuery> extends BuiltQuery {

    T query;

    BuiltQueryDecorator(T query) {
        super(null);
        this.query = query;
    }

    @Override
    public String getCommand() {
        return query.getCommand();
    }

    @Override
    StringBuilder buildQueryString() {
        return query.buildQueryString();
    }

    @Override
    public String getDatabase() {
        return query.getDatabase();
    }

    @Override
    public String toString() {
        return query.toString();
    }

}
