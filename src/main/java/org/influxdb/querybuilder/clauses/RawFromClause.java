package org.influxdb.querybuilder.clauses;

public class RawFromClause extends FromClause {

  private final String text;

  public RawFromClause(final String text) {
    if (text == null) {
      throw new IllegalArgumentException("Provide a valid value");
    }
    this.text = text;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    stringBuilder.append(text);
  }
}
