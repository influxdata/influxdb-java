package org.influxdb.querybuilder;

public class Ordering implements Appendable {

  private final boolean isDesc;

  private static final String TIME_KEY = "time";

  /**
   * Influxdb ordering currently supports only time.
   *
   * @param isDesc
   */
  Ordering(final boolean isDesc) {
    this.isDesc = isDesc;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    Appender.appendName(TIME_KEY, stringBuilder);
    if (isDesc) {
      stringBuilder.append(" DESC");
    } else {
      stringBuilder.append(" ASC");
    }
  }
}
