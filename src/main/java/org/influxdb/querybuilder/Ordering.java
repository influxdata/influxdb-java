package org.influxdb.querybuilder;

public class Ordering implements Appendable {

  private final boolean isDesc;

  private static final String TIME_KEY = "time";

  /**
   * Influxdb ordering currently supports onlye time
   *
   * @param isDesc
   */
  public Ordering(boolean isDesc) {
    this.isDesc = isDesc;
  }

  @Override
  public void appendTo(StringBuilder sb) {
    Appender.appendName(TIME_KEY, sb);
    sb.append(isDesc ? " DESC" : " ASC");
  }
}
