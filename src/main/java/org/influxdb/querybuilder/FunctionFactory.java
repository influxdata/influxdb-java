package org.influxdb.querybuilder;

public class FunctionFactory {

    public static Object function(String name, Object... parameters) {
        toColumns(parameters);
        return new Function(name, parameters);
    }

    public static Object now() {
        return new Function("now");
    }

    public static Object count(Object column) {
        return new Function("COUNT", toColumn(column));
    }

    public static Object max(Object column) {
        return new Function("MAX", toColumn(column));
    }

    public static Object min(Object column) {
        return new Function("MIN", toColumn(column));
    }

    public static Object sum(Object column) {
        return new Function("SUM", toColumn(column));
    }

    public static Object mean(Object column) {
        return new Function("MEAN", toColumn(column));
    }

    public static Object column(String name) {
        return new Column(name);
    }

    private static void toColumns(Object... arguments) {
        for(int i=0;i<arguments.length;i++) {
            arguments[i] = toColumn(arguments[i]);
        }
    }

    private static Object toColumn(Object argument) {
        if (argument instanceof String)
            return column(((String) argument));
        return argument;
    }

}
