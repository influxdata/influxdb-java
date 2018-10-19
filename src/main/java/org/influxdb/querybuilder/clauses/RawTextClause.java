package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.Appender;
import org.influxdb.querybuilder.RawText;

public class RawTextClause extends AbstractClause {

  private final RawText value;

  public RawTextClause(final String text) {
    super("");
    this.value = new RawText(text);

    if (text == null) {
      throw new IllegalArgumentException("Missing text for expression");
    }
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    Appender.appendValue(value, stringBuilder);
  }
}
