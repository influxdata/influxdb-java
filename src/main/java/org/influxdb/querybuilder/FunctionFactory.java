package org.influxdb.querybuilder;

import static org.influxdb.querybuilder.Aggregations.COUNT;
import static org.influxdb.querybuilder.Aggregations.MAX;
import static org.influxdb.querybuilder.Aggregations.MEAN;
import static org.influxdb.querybuilder.Aggregations.MIN;
import static org.influxdb.querybuilder.Aggregations.SUM;

import org.influxdb.querybuilder.time.TimeInterval;

public final class FunctionFactory {

  private FunctionFactory() {
  }

  public static Object function(final String name, final Object... parameters) {
    convertToColumns(parameters);
    return new Function(name, parameters);
  }

  public static Object now() {
    return new Function("now");
  }

  public static Object count(final Object column) {
    return new Function(COUNT, convertToColumn(column));
  }

  public static Object max(final Object column) {
    return new Function(MAX, convertToColumn(column));
  }

  public static Object min(final Object column) {
    return new Function(MIN, convertToColumn(column));
  }

  public static Object sum(final Object column) {
    return new Function(SUM, convertToColumn(column));
  }

  public static Object mean(final Object column) {
    return new Function(MEAN, convertToColumn(column));
  }

  public static Object time(Long timeInterval, String durationLiteral) {
    return new Function("time", new TimeInterval(timeInterval, durationLiteral));
  }

  public static Object time(
      Long timeInterval, String durationLiteral, Long offsetInterval, String offSetLiteral) {
    return new Function(
        "time",
        new TimeInterval(timeInterval, durationLiteral),
        new TimeInterval(offsetInterval, offSetLiteral));
  }

  public static Object column(final String name) {
    return new Column(name);
  }

  private static void convertToColumns(final Object... arguments) {
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = convertToColumn(arguments[i]);
    }
  }

  private static Object convertToColumn(final Object argument) {
    if (argument instanceof String) {
      return column(((String) argument));
    }

    return argument;
  }
}
