package org.influxdb.querybuilder;

public class RawText implements Appendable {

  private final String text;

  public RawText(final String text) {
    this.text = text;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    stringBuilder.append(text);
  }
}
