package org.influxdb.querybuilder;

import java.util.List;
import java.util.regex.Pattern;

public class Appender {

    private static final Pattern columnNamePattern = Pattern.compile("\\w+(?:\\[.+\\])?");

    public static StringBuilder joinAndAppend(StringBuilder stringBuilder, String separator, List<? extends Appendable> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                stringBuilder.append(separator);
            values.get(i).appendTo(stringBuilder);
        }
        return stringBuilder;
    }

    public static StringBuilder joinAndAppendNames(StringBuilder stringBuilder, List<?> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                stringBuilder.append(",");
            appendName(values.get(i), stringBuilder);
        }
        return stringBuilder;
    }

    public static StringBuilder appendValue(Object value, StringBuilder stringBuilder) {
        if (value == null) {
            stringBuilder.append("null");
        } else if (value instanceof Function) {
            Function fcall = (Function) value;
            stringBuilder.append(fcall.getName()).append('(');
            for (int i = 0; i < fcall.getParameters().length; i++) {
                if (i > 0)
                    stringBuilder.append(',');
                appendValue(fcall.getParameters()[i], stringBuilder);
            }
            stringBuilder.append(')');
        } else if (value instanceof Column) {
            appendName(((Column) value).getName(), stringBuilder);
        } else if (value instanceof RawString) {
            stringBuilder.append(value.toString());
        } else if (value instanceof String) {
            stringBuilder.append("'").append(value).append("'");
        } else if (value != null) {
            stringBuilder.append(value);
        } else {
            stringBuilder.append('?');
            return stringBuilder;
        }
        return stringBuilder;
    }

    public static StringBuilder appendName(String name, StringBuilder stringBuilder) {
        name = name.trim();
        if (name.startsWith("\"") || columnNamePattern.matcher(name).matches()) {
            stringBuilder.append(name);
        } else {
            stringBuilder.append('"').append(name).append('"');
        }
        return stringBuilder;
    }

    public static StringBuilder appendName(Object name, StringBuilder stringBuilder) {
        if (name instanceof String) {
            appendName((String) name, stringBuilder);
        } else if (name instanceof Column) {
            appendName(((Column) name).getName(), stringBuilder);
        } else if (name instanceof Function) {
            Function functionCall = (Function) name;
            stringBuilder.append(functionCall.getName()).append('(');
            for (int i = 0; i < functionCall.getParameters().length; i++) {
                if (i > 0)
                    stringBuilder.append(',');
                appendValue(functionCall.getParameters()[i], stringBuilder);
            }
            stringBuilder.append(')');
        } else if (name instanceof Alias) {
            Alias alias = (Alias) name;
            appendName(alias.getColumn(), stringBuilder);
            stringBuilder.append(" AS ").append(alias.getAlias());
        } else if (name instanceof RawString) {
            stringBuilder.append(name);
        } else if (name instanceof Distinct) {
            Distinct distinct = (Distinct) name;
            stringBuilder.append("DISTINCT ");
            appendName(distinct.getExpression(), stringBuilder);
        } else {
            throw new IllegalArgumentException(String.format("Invalid column %s of type unknown of the query builder", name));
        }
        return stringBuilder;
    }

}