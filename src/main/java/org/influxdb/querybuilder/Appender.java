package org.influxdb.querybuilder;

import java.util.List;
import java.util.regex.Pattern;
import org.influxdb.querybuilder.clauses.ConjunctionClause;

public final class Appender {

  private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile("\\w+(?:\\[.+\\])?");

  private Appender() {
  }

  public static StringBuilder joinAndAppend(
      final StringBuilder stringBuilder, final List<? extends ConjunctionClause> clauses) {
    for (int i = 0; i < clauses.size(); i++) {
      if (i > 0) {
        clauses.get(i).join(stringBuilder);
      }
      clauses.get(i).appendTo(stringBuilder);
    }
    return stringBuilder;
  }

  public static StringBuilder joinAndAppend(
      final StringBuilder stringBuilder,
      final String separator,
      final List<? extends Appendable> values) {
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        stringBuilder.append(separator);
      }
      values.get(i).appendTo(stringBuilder);
    }
    return stringBuilder;
  }

  public static StringBuilder joinAndAppendNames(
      final StringBuilder stringBuilder, final List<?> values) {
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        stringBuilder.append(",");
      }
      appendName(values.get(i), stringBuilder);
    }
    return stringBuilder;
  }

  public static StringBuilder appendValue(final Object value, final StringBuilder stringBuilder) {
    if (value instanceof Appendable) {
      Appendable appendable = (Appendable) value;
      appendable.appendTo(stringBuilder);
    } else if (value instanceof Function) {
      Function functionCall = (Function) value;
      stringBuilder.append(functionCall.getName()).append('(');
      for (int i = 0; i < functionCall.getParameters().length; i++) {
        if (i > 0) {
          stringBuilder.append(',');
        }
        appendValue(functionCall.getParameters()[i], stringBuilder);
      }
      stringBuilder.append(')');
    } else if (value instanceof Column) {
      appendName(((Column) value).getName(), stringBuilder);
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

  public static StringBuilder appendName(final String name, final StringBuilder stringBuilder) {
    String trimmedName = name.trim();
    if (trimmedName.startsWith("\"") || COLUMN_NAME_PATTERN.matcher(trimmedName).matches()) {
      stringBuilder.append(trimmedName);
    } else {
      stringBuilder.append('"').append(trimmedName).append('"');
    }
    return stringBuilder;
  }

  public static StringBuilder appendName(final Object name, final StringBuilder stringBuilder) {
    if (name instanceof String) {
      appendName((String) name, stringBuilder);
    } else if (name instanceof Column) {
      appendName(((Column) name).getName(), stringBuilder);
    } else if (name instanceof Function) {
      Function functionCall = (Function) name;
      stringBuilder.append(functionCall.getName()).append('(');
      for (int i = 0; i < functionCall.getParameters().length; i++) {
        if (i > 0) {
          stringBuilder.append(',');
        }
        appendValue(functionCall.getParameters()[i], stringBuilder);
      }
      stringBuilder.append(')');
    } else if (name instanceof Alias) {
      Alias alias = (Alias) name;
      appendName(alias.getColumn(), stringBuilder);
      stringBuilder.append(" AS ").append(alias.getAlias());
    } else if (name instanceof Distinct) {
      Distinct distinct = (Distinct) name;
      stringBuilder.append("DISTINCT ");
      appendName(distinct.getExpression(), stringBuilder);
    } else if (name instanceof Appendable) {
      Appendable appendable = (Appendable) name;
      appendable.appendTo(stringBuilder);
    } else {
      throw new IllegalArgumentException("Invalid type");
    }
    return stringBuilder;
  }
}
