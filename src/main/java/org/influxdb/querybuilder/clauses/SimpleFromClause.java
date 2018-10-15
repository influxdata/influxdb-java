package org.influxdb.querybuilder.clauses;

import static org.influxdb.querybuilder.Appender.appendName;

public class SimpleFromClause extends FromClause {

  private final String table;

  public SimpleFromClause(final String table) {
    if (table == null) {
      throw new IllegalArgumentException("Provide a valid table name");
    }
    this.table = table;
  }

  @Override
  public void appendTo(final StringBuilder stringBuilder) {
    appendName(table, stringBuilder);
  }
}
