package org.influxdb.querybuilder;

public class TimeZone implements Appendable {

  private final String timeZone;

  TimeZone(final String timeZone) {
    this.timeZone = timeZone;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
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
