package org.influxdb.querybuilder;

public class FunctionFactory {

    public static Object fcall(String name, Object... parameters) {
        return new Function(name, parameters);
    }

    public static Object now() {
        return new Function("now");
    }

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

    public static Object column(String name) {
        return new Column(name);
    }

}
