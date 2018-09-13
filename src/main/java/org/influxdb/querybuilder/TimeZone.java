package org.influxdb.querybuilder;

public class TimeZone implements Appendable {

  private final String timeZone;

  public TimeZone(final String timeZone) {
    this.timeZone = timeZone;
  }

  @Override
  public void appendTo(StringBuilder stringBuilder) {
    stringBuilder
        .append(" ")
        .append("tz")
        .append("(")
        .append("'")
        .append(timeZone)
        .append("'")
        .append(")")
        .append(" ");
  }
}
