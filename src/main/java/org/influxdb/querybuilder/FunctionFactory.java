package org.influxdb.querybuilder;

import static org.influxdb.querybuilder.Aggregations.*;

public class FunctionFactory {

    public static Object function(String name, Object... parameters) {
        convertToColumns(parameters);
        return new Function(name, parameters);
    }

    public static Object now() {
        return new Function("now");
    }

    public static Object count(Object column) {
        return new Function(COUNT, convertToColumn(column));
    }

    public static Object max(Object column) {
        return new Function(MAX, convertToColumn(column));
    }

    public static Object min(Object column) {
        return new Function(MIN, convertToColumn(column));
    }

    public static Object sum(Object column) {
        return new Function(SUM, convertToColumn(column));
    }

    public static Object mean(Object column) {
        return new Function(MEAN, convertToColumn(column));
    }

    public static Object column(String name) {
        return new Column(name);
    }

    private static void convertToColumns(Object... arguments) {
        for(int i=0;i<arguments.length;i++) {
            arguments[i] = convertToColumn(arguments[i]);
        }
    }

    private static Object convertToColumn(Object argument) {
        if (argument instanceof String)
            return column(((String) argument));
        return argument;
    }

}
