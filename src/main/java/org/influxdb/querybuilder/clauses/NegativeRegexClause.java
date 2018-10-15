package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.Appender;
import org.influxdb.querybuilder.Operations;
import org.influxdb.querybuilder.RawText;

public class NegativeRegexClause extends AbstractClause {

  private final RawText value;

  public NegativeRegexClause(final String name, final String value) {
    super(name);
    this.value = new RawText(value);

    if (value == null) {
      throw new IllegalArgumentException("Missing value for regex clause");
    }
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    Appender.appendName(name, stringBuilder).append(" ").append(Operations.NER).append(" ");
    Appender.appendValue(value, stringBuilder);
  }
}
