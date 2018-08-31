package org.influxdb.querybuilder;

public class RawText {

  private final String text;

  public RawText(final String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return text;
  }
}
