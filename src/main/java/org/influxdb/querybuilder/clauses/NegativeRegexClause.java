package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.Appender;
import org.influxdb.querybuilder.Operations;
import org.influxdb.querybuilder.RawString;

public class NegativeRegexClause extends AbstractClause {

  private final RawString value;

  public NegativeRegexClause(String name, String value) {
    super(name);
    this.value = new RawString(value);

    if (value == null) throw new IllegalArgumentException("Missing value for regex clause");
  }

  @Override
  public void appendTo(StringBuilder sb) {
    Appender.appendName(name, sb).append(" ").append(Operations.NER).append(" ");
    Appender.appendValue(value, sb);
  }
}
