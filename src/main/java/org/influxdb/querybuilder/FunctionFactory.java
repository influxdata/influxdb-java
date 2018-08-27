package org.influxdb.querybuilder;

public class FunctionFactory {

    public static Object count(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("COUNT", column);
    }

    public static Object max(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("MAX", column);
    }

    public static Object min(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("MIN", column);
    }

    public static Object sum(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("SUM", column);
    }

    public static Object mean(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("MEAN", column);
    }

    private static Object column(String name) {
        return new Column(name);
    }

}
