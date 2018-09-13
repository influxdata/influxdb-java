package org.influxdb.querybuilder.clauses;

import static org.influxdb.querybuilder.Appender.appendName;
import static org.influxdb.querybuilder.Appender.appendValue;

import org.influxdb.querybuilder.Operations;
import org.influxdb.querybuilder.RawText;

public class RegexClause extends AbstractClause {

  private final RawText value;

  public RegexClause(final String name, final String value) {
    super(name);
    this.value = new RawText(value);

    if (value == null) {
      throw new IllegalArgumentException("Missing value for regex clause");
    }
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    appendName(name, stringBuilder).append(" ").append(Operations.EQR).append(" ");
    appendValue(value, stringBuilder);
  }
}
